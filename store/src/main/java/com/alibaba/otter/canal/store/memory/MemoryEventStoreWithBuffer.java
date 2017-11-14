package com.alibaba.otter.canal.store.memory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.CanalStoreScavenge;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

/**
 * 基于内存buffer构建内存memory store
 * 
 * <pre>
 * 变更记录：
 * 1. 新增BatchMode类型，支持按内存大小获取批次数据，内存大小更加可控.
 *   a. put操作，会首先根据bufferSize进行控制，然后再进行bufferSize * bufferMemUnit进行控制. 因存储的内容是以Event，如果纯依赖于memsize进行控制，会导致RingBuffer出现动态伸缩
 * </pre>
 * 
 * @author jianghang 2012-6-20 上午09:46:31
 * @version 1.0.0
 */
public class MemoryEventStoreWithBuffer extends AbstractCanalStoreScavenge implements CanalEventStore<Event>, CanalStoreScavenge {

    private static final long INIT_SQEUENCE = -1;
    private int               bufferSize    = 16 * 1024;//队列的size
    private int               bufferMemUnit = 1024;                         // memsize的单位，默认为1kb大小
    private int               indexMask;
    private Event[]           entries;//存储事件的队列

    // 记录下put/get/ack操作的三个下标
    //顺序一定是ack   get  put,因为put一定是最大的,数据先进入到buffer中的,然后才能get消费掉,最终客户端提示get消费的数据已经确定了,因此才有的ack
    private AtomicLong        putSequence   = new AtomicLong(INIT_SQEUENCE); // 代表当前put操作最后一次写操作发生的位置
    private AtomicLong        getSequence   = new AtomicLong(INIT_SQEUENCE); // 代表当前get操作读取的最后一条的位置
    private AtomicLong        ackSequence   = new AtomicLong(INIT_SQEUENCE); // 代表当前ack操作的最后一条的位置

    // 记录下put/get/ack操作的三个memsize大小
    private AtomicLong        putMemSize    = new AtomicLong(0);
    private AtomicLong        getMemSize    = new AtomicLong(0);//已经get到多少内存
    private AtomicLong        ackMemSize    = new AtomicLong(0);

    // 阻塞put/get操作控制信号
    private ReentrantLock     lock          = new ReentrantLock();
    private Condition         notFull       = lock.newCondition();//当非满的时候使用,即已经get完成了.有空间进行put了
    private Condition         notEmpty      = lock.newCondition();//当非空的时候,即put到数据了,可以get了,因此此时时候

    private BatchMode         batchMode     = BatchMode.ITEMSIZE;           // 默认为内存大小模式
    private boolean           ddlIsolation  = false;//针对DDL的处理,true表示一次get的请求只能是一个DDL,不能有其他数据

    public MemoryEventStoreWithBuffer(){

    }

    public MemoryEventStoreWithBuffer(BatchMode batchMode){
        this.batchMode = batchMode;
    }

    public void start() throws CanalStoreException {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {//必须是2的整数倍
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        indexMask = bufferSize - 1;
        entries = new Event[bufferSize];
    }

    public void stop() throws CanalStoreException {
        super.stop();

        cleanAll();
    }

    //不断添加数据,一直到添加成功未知,队列满了就等待
    public void put(List<Event> data) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return;
        }

        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkFreeSlotAt(putSequence.get() + data.size())) { // 检查是否有空位
                    notFull.await(); // wait until not full 没有空位置了,则等待
                }
            } catch (InterruptedException ie) {
                notFull.signal(); // propagate to non-interrupted thread
                throw ie;
            }
            doPut(data);//真正的存放数据
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } finally {
            lock.unlock();
        }
    }

    //直到timeout还没有存储成功,则返回false
    public boolean put(List<Event> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }

        long nanos = unit.toNanos(timeout);//转换时间单位
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                if (checkFreeSlotAt(putSequence.get() + data.size())) {
                    doPut(data);
                    return true;
                }
                if (nanos <= 0) {
                    return false;
                }

                try {
                    nanos = notFull.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notFull.signal(); // propagate to non-interrupted thread
                    throw ie;//抛出异常,退出put操作
                }
            }
        } finally {
            lock.unlock();
        }
    }

    //只会添加一次,队列满了就不用等待了,直接退出,返回false
    public boolean tryPut(List<Event> data) throws CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (!checkFreeSlotAt(putSequence.get() + data.size())) {
                return false;
            } else {
                doPut(data);
                return true;
            }
        } finally {
            lock.unlock();
        }
    }

    public void put(Event data) throws InterruptedException, CanalStoreException {
        put(Arrays.asList(data));
    }

    public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        return put(Arrays.asList(data), timeout, unit);
    }

    public boolean tryPut(Event data) throws CanalStoreException {
        return tryPut(Arrays.asList(data));
    }

    /**
     * 执行具体的put操作
     */
    private void doPut(List<Event> data) {
        long current = putSequence.get();
        long end = current + data.size();

        // 先写数据，再更新对应的cursor,并发度高的情况，putSequence会被get请求可见，拿出了ringbuffer中的老的Entry值
        for (long next = current + 1; next <= end; next++) {
            entries[getIndex(next)] = data.get((int) (next - current - 1));//将每一个data内的数据添加到entries中
        }

        putSequence.set(end);//设置最新的位置--put到哪里了

        // 记录一下gets memsize信息，方便快速检索
        if (batchMode.isMemSize()) {
            long size = 0;
            for (Event event : data) {
                size += calculateSize(event);
            }

            putMemSize.getAndAdd(size);//计算put的size大小
        }

        // tell other threads that store is not empty
        notEmpty.signal();
    }

    //从哪个队列位置开始获取数据,获取多少条数据
    public Events<Event> get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkUnGetSlotAt((LogPosition) start, batchSize))
                    notEmpty.await();
            } catch (InterruptedException ie) {
                notEmpty.signal(); // propagate to non-interrupted thread
                throw ie;
            }

            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    //从哪个队列位置开始获取数据,获取多少条数据,如果timeout还没有获取完,则有多少获取多少
    public Events<Event> get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException,
                                                                                        CanalStoreException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                if (checkUnGetSlotAt((LogPosition) start, batchSize)) {
                    return doGet(start, batchSize);
                }

                if (nanos <= 0) {
                    // 如果时间到了，有多少取多少
                    return doGet(start, batchSize);
                }

                try {
                    nanos = notEmpty.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notEmpty.signal(); // propagate to non-interrupted thread
                    throw ie;
                }

            }
        } finally {
            lock.unlock();
        }
    }

    //有多少获取多少,不需要获取batchSize个数据后在返回
    public Events<Event> tryGet(Position start, int batchSize) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    private Events<Event> doGet(Position start, int batchSize) throws CanalStoreException {
        LogPosition startPosition = (LogPosition) start;

        long current = getSequence.get();//上次获取到哪里了
        long maxAbleSequence = putSequence.get();//目前写到哪里了
        long next = current;
        long end = current;
        // 如果startPosition为null，说明是第一次，默认+1处理
        if (startPosition == null || !startPosition.getPostion().isIncluded()) { // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
            next = next + 1;//从接下来的位置开始抓去
        }

        if (current >= maxAbleSequence) {//说明没有空间去get
            return new Events<Event>();
        }

        Events<Event> result = new Events<Event>();
        List<Event> entrys = result.getEvents();//存储get到的数据集合
        long memsize = 0;//已经读取到的内存和
        if (batchMode.isItemSize()) {//基于数量
            end = (next + batchSize - 1) < maxAbleSequence ? (next + batchSize - 1) : maxAbleSequence;//要么全读取,要么读取batchSize条数据
            // 提取数据并返回
            for (; next <= end; next++) {
                Event event = entries[getIndex(next)];//获取事件
                if (ddlIsolation && isDdl(event.getEntry().getHeader().getEventType())) {//说明是DDL事件
                    // 如果是ddl隔离，直接返回
                    if (entrys.size() == 0) {//说明第一条就是DDL事件,则停止get
                        entrys.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置  说明接下来的是DDL事件,则不读去该事件,直接返回已经读取到的非DDL事件
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    entrys.add(event);//将事件加入到队列中---可能包含DDL事件等所有事件
                }
            }
        } else {//基于内存
            long maxMemSize = batchSize * bufferMemUnit;//此时batchSize就是内存大小,单位是K
            for (; memsize <= maxMemSize && next <= maxAbleSequence; next++) {//只要内存不达到,就不断的添加数据即可
                // 永远保证可以取出第一条的记录，避免死锁
                Event event = entries[getIndex(next)];
                if (ddlIsolation && isDdl(event.getEntry().getHeader().getEventType())) {//是否是DDL操作
                    // 如果是ddl隔离，直接返回
                    if (entrys.size() == 0) {
                        entrys.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    entrys.add(event);
                    memsize += calculateSize(event);
                    end = next;// 记录end位点
                }
            }

        }

        //设置查询出来的事件position位置的区间
        PositionRange<LogPosition> range = new PositionRange<LogPosition>();
        result.setPositionRange(range);

        range.setStart(CanalEventUtils.createPosition(entrys.get(0)));
        range.setEnd(CanalEventUtils.createPosition(entrys.get(result.getEvents().size() - 1)));

        // 记录一下是否存在可以被ack的点
        for (int i = entrys.size() - 1; i >= 0; i--) {//找到最后一个事件是事务相关的事件位置,即多个事件集合中,可能存在多个事务.因此获取最后一个事务出现的位置即可
            Event event = entrys.get(i);
            if (CanalEntry.EntryType.TRANSACTIONBEGIN == event.getEntry().getEntryType()//出现事务的位置
                || CanalEntry.EntryType.TRANSACTIONEND == event.getEntry().getEntryType()
                || isDdl(event.getEntry().getHeader().getEventType())) {//出现DDL操作的位置
                // 将事务头/尾设置可被为ack的点
                range.setAck(CanalEventUtils.createPosition(event));
                break;
            }
        }

        if (getSequence.compareAndSet(current, end)) {//说明此时get到的数据已经到end了
            getMemSize.addAndGet(memsize);
            notFull.signal();
            return result;
        } else {
            return new Events<Event>();
        }
    }

    public LogPosition getFirstPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long firstSeqeuence = ackSequence.get();
            if (firstSeqeuence == INIT_SQEUENCE && firstSeqeuence < putSequence.get()) {
                // 没有ack过数据
                Event event = entries[getIndex(firstSeqeuence + 1)]; // 最后一次ack为-1，需要移动到下一条,included
                                                                     // = false
                return CanalEventUtils.createPosition(event, false);
            } else if (firstSeqeuence > INIT_SQEUENCE && firstSeqeuence < putSequence.get()) {
                // ack未追上put操作
                Event event = entries[getIndex(firstSeqeuence + 1)]; // 最后一次ack的位置数据
                                                                     // + 1
                return CanalEventUtils.createPosition(event, true);
            } else if (firstSeqeuence > INIT_SQEUENCE && firstSeqeuence == putSequence.get()) {
                // 已经追上，store中没有数据
                Event event = entries[getIndex(firstSeqeuence)]; // 最后一次ack的位置数据，和last为同一条，included
                                                                 // = false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 没有任何数据
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public LogPosition getLatestPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long latestSequence = putSequence.get();
            if (latestSequence > INIT_SQEUENCE && latestSequence != ackSequence.get()) {
                Event event = entries[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，最后一条未消费的数据
                return CanalEventUtils.createPosition(event, true);
            } else if (latestSequence > INIT_SQEUENCE && latestSequence == ackSequence.get()) {
                // ack已经追上了put操作
                Event event = entries[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，included
                                                                            // =
                                                                            // false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 没有任何数据
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public void ack(Position position) throws CanalStoreException {
        cleanUntil(position);
    }

    //说明该位置已经提交了
    public void cleanUntil(Position position) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long sequence = ackSequence.get();
            long maxSequence = getSequence.get();

            boolean hasMatch = false;//true表示已经确定找到该点了
            long memsize = 0;
            for (long next = sequence + 1; next <= maxSequence; next++) {//从确定ack的位置开始循环,一直到get的位置结束,即消费到的点结束
                Event event = entries[getIndex(next)];//获取每一个事件
                memsize += calculateSize(event);
                boolean match = CanalEventUtils.checkPosition(event, (LogPosition) position);//找到要清理的哪个点
                if (match) {// 找到对应的position，更新ack seq
                    hasMatch = true;

                    if (batchMode.isMemSize()) {
                        ackMemSize.addAndGet(memsize);
                        // 尝试清空buffer中的内存，将ack之前的内存全部释放掉
                        for (long index = sequence + 1; index < next; index++) {
                            entries[getIndex(index)] = null;// 设置为null
                        }
                    }

                    if (ackSequence.compareAndSet(sequence, next)) {// 避免并发ack
                        notFull.signal();
                        return;
                    }
                }
            }

            if (!hasMatch) {// 找不到对应需要ack的position
                throw new CanalStoreException("no match ack position" + position.toString());
            }
        } finally {
            lock.unlock();
        }
    }

    //回滚,即get位置从acl位置开始重新计算
    public void rollback() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            getSequence.set(ackSequence.get());
            getMemSize.set(ackMemSize.get());//内存就是ack那个时间点的内存
        } finally {
            lock.unlock();
        }
    }

    //清理所有的数据
    public void cleanAll() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            putSequence.set(INIT_SQEUENCE);
            getSequence.set(INIT_SQEUENCE);
            ackSequence.set(INIT_SQEUENCE);

            putMemSize.set(0);
            getMemSize.set(0);
            ackMemSize.set(0);
            entries = null;
            // for (int i = 0; i < entries.length; i++) {
            // entries[i] = null;
            // }
        } finally {
            lock.unlock();
        }
    }

    // =================== helper method =================
    //获取最小的可用的位置
    private long getMinimumGetOrAck() {
        long get = getSequence.get();//get到哪里了
        long ack = ackSequence.get();//确定的是到哪里了
        return ack <= get ? ack : get;
    }

    /**
     * 查询是否有空位--用于put
     * false表示队列满了,不能在put新元素了
     */
    private boolean checkFreeSlotAt(final long sequence) {
        final long wrapPoint = sequence - bufferSize;//该值>0,表示序号已经比buff大了,并且大了多少个
        final long minPoint = getMinimumGetOrAck();
        //比如wrapPoint=100,说明比buffer多了100,因此要向buffer的从0-100之间填写内容了
        //而minPoint = 80,说明从81之后的数字都不能被消费,显然80-100之间是没办法满足这20个坑的,因此要返回false
        if (wrapPoint > minPoint) { // 刚好追上一轮
            return false;
        } else {//说明坑够了,但是要继续看内存是否够
            // 在bufferSize模式上，再增加memSize控制
            if (batchMode.isMemSize()) {
                final long memsize = putMemSize.get() - ackMemSize.get();//已经使用的内存
                if (memsize < bufferSize * bufferMemUnit) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }
    }

    /**
     * 检查是否存在需要get的数据,并且数量>=batchSize---用于get
     */
    private boolean checkUnGetSlotAt(LogPosition startPosition, int batchSize) {
        if (batchMode.isItemSize()) {
            long current = getSequence.get();
            long maxAbleSequence = putSequence.get();
            long next = current;
            if (startPosition == null || !startPosition.getPostion().isIncluded()) { // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
                next = next + 1;// 少一条数据
            }

            if (current < maxAbleSequence && next + batchSize - 1 <= maxAbleSequence) {
                return true;
            } else {
                return false;
            }
        } else {
            // 处理内存大小判断
            long currentSize = getMemSize.get();
            long maxAbleSize = putMemSize.get();

            if (maxAbleSize - currentSize >= batchSize * bufferMemUnit) {
                return true;
            } else {
                return false;
            }
        }
    }

    //计算每一个事件的字节大小
    private long calculateSize(Event event) {
        // 直接返回binlog中的事件大小
        return event.getEntry().getHeader().getEventLength();
    }

    private int getIndex(long sequcnce) {
        return (int) sequcnce & indexMask;
    }

    //是否是ddl类型
    private boolean isDdl(EventType type) {
        return type == EventType.ALTER || type == EventType.CREATE || type == EventType.ERASE
               || type == EventType.RENAME || type == EventType.TRUNCATE || type == EventType.CINDEX
               || type == EventType.DINDEX;
    }

    // ================ setter / getter ==================

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setBufferMemUnit(int bufferMemUnit) {
        this.bufferMemUnit = bufferMemUnit;
    }

    public void setBatchMode(BatchMode batchMode) {
        this.batchMode = batchMode;
    }

    public void setDdlIsolation(boolean ddlIsolation) {
        this.ddlIsolation = ddlIsolation;
    }

}
