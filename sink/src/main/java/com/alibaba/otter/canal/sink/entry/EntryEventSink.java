package com.alibaba.otter.canal.sink.entry;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.sink.AbstractCanalEventSink;
import com.alibaba.otter.canal.sink.CanalEventDownStreamHandler;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;

/**
 * mysql binlog数据对象输出
 * 
 * @author jianghang 2012-7-4 下午03:23:16
 * @version 1.0.0
 */
public class EntryEventSink extends AbstractCanalEventSink<List<CanalEntry.Entry>> implements CanalEventSink<List<CanalEntry.Entry>> {

    private static final Logger    logger                        = LoggerFactory.getLogger(EntryEventSink.class);
    private static final int       maxFullTimes                  = 10;
    private CanalEventStore<Event> eventStore;//事件存储容器
    protected boolean              filterTransactionEntry        = false;                                        // 是否需要过滤事务头/尾,true表示不要事务开始、事务结束事件
    protected boolean              filterEmtryTransactionEntry   = true;                                         // 是否需要过滤空的事务头/尾,true表示事件只有开始和结束,没有数据内容的事件,要被过滤掉,不去执行
    //两种阀值去控制空的事件去sink处理
    protected long                 emptyTransactionInterval      = 5 * 1000;                                     // 空的事务输出的频率
    protected long                 emptyTransctionThresold       = 8192;                                         // 超过1024个事务头，输出一个
    //针对emptyTransactionInterval和emptyTransctionThresold的计数器
    protected volatile long        lastEmptyTransactionTimestamp = 0L;
    protected AtomicLong           lastEmptyTransactionCount     = new AtomicLong(0L);

    public EntryEventSink(){
        addHandler(new HeartBeatEntryEventHandler());//添加一个将心跳事件刨除的处理拦截器
    }

    public void start() {
        super.start();
        Assert.notNull(eventStore);

        //开启所有的拦截器
        for (CanalEventDownStreamHandler handler : getHandlers()) {
            if (!handler.isStart()) {
                handler.start();
            }
        }
    }

    public void stop() {
        super.stop();

        //关闭所有的拦截器
        for (CanalEventDownStreamHandler handler : getHandlers()) {
            if (handler.isStart()) {
                handler.stop();
            }
        }
    }

    public boolean filter(List<Entry> event, InetSocketAddress remoteAddress, String destination) {

        return false;
    }

    public boolean sink(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                           throws CanalSinkException,
                                                                                                           InterruptedException {
        List rowDatas = entrys;
        if (filterTransactionEntry) {//true表示不要事务开始、事务结束事件
            rowDatas = new ArrayList<CanalEntry.Entry>();
            for (CanalEntry.Entry entry : entrys) {
                if (entry.getEntryType() == EntryType.ROWDATA) {//只要ROWDATA事件
                    rowDatas.add(entry);
                }
            }
        }

        return sinkData(rowDatas, remoteAddress);
    }

    private boolean sinkData(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress)
                                                                                            throws InterruptedException {
        boolean hasRowData = false;//true表示有行事件
        boolean hasHeartBeat = false;//true表示有心跳事件
        List<Event> events = new ArrayList<Event>();//通过的事件集合
        for (CanalEntry.Entry entry : entrys) {
            Event event = new Event(new LogIdentity(remoteAddress, -1L), entry);
            if (!doFilter(event)) {//如果一个事件是过滤器返回false,则说明不需要存储该事件,因此continue处理下一个事件
                continue;
            }

            events.add(event);
            hasRowData |= (entry.getEntryType() == EntryType.ROWDATA);
            hasHeartBeat |= (entry.getEntryType() == EntryType.HEARTBEAT);
        }

        if (hasRowData) {
            // 存在row记录
            return doSink(events);
        } else if (hasHeartBeat) {
            // 存在heartbeat记录，直接跳给后续处理
            return doSink(events);
        } else {
            // 需要过滤的数据
            if (filterEmtryTransactionEntry && !CollectionUtils.isEmpty(events)) {//true表示事件只有开始和结束,没有数据内容的事件,要被过滤掉,不去执行
                long currentTimestamp = events.get(0).getEntry().getHeader().getExecuteTime();//第一个事件的执行时间
                // 基于一定的策略控制，放过空的事务头和尾，便于及时更新数据库位点，表明工作正常
                if (Math.abs(currentTimestamp - lastEmptyTransactionTimestamp) > emptyTransactionInterval
                    || lastEmptyTransactionCount.incrementAndGet() > emptyTransctionThresold) {
                    lastEmptyTransactionCount.set(0L);
                    lastEmptyTransactionTimestamp = currentTimestamp;
                    return doSink(events);
                }
            }

            // 直接返回true，忽略空的事务头和尾
            return true;
        }
    }

    //对数据库.table进行过滤
    //true表示过滤成功,选择要处理该表,false表示要对该事件进行丢弃掉
    protected boolean doFilter(Event event) {
        if (filter != null && event.getEntry().getEntryType() == EntryType.ROWDATA) {
            String name = getSchemaNameAndTableName(event.getEntry());
            boolean need = filter.filter(name);
            if (!need) {
                logger.debug("filter name[{}] entry : {}:{}",
                    name,
                    event.getEntry().getHeader().getLogfileName(),
                    event.getEntry().getHeader().getLogfileOffset());
            }

            return need;
        } else {
            return true;
        }
    }

    protected boolean doSink(List<Event> events) {
        for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
            events = handler.before(events);
        }

        int fullTimes = 0;
        do {
            if (eventStore.tryPut(events)) {
                for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
                    events = handler.after(events);
                }
                return true;
            } else {
                applyWait(++fullTimes);
            }

            for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
                events = handler.retry(events);
            }

        } while (running && !Thread.interrupted());
        return false;
    }

    // 处理无数据的情况，避免空循环挂死
    private void applyWait(int fullTimes) {
        int newFullTimes = fullTimes > maxFullTimes ? maxFullTimes : fullTimes;
        if (fullTimes <= 3) { // 3次以内
            Thread.yield();//让线程让开cpu,让别人去执行,不需要等待
        } else { // 超过3次，最多只sleep 10ms
            LockSupport.parkNanos(1000 * 1000L * newFullTimes);//超过三次后,就睡眠
        }

    }

    //返回数据库.table名字
    private String getSchemaNameAndTableName(CanalEntry.Entry entry) {
        return entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
    }

    public void setEventStore(CanalEventStore<Event> eventStore) {
        this.eventStore = eventStore;
    }

    public void setFilterTransactionEntry(boolean filterTransactionEntry) {
        this.filterTransactionEntry = filterTransactionEntry;
    }

    public void setFilterEmtryTransactionEntry(boolean filterEmtryTransactionEntry) {
        this.filterEmtryTransactionEntry = filterEmtryTransactionEntry;
    }

    public void setEmptyTransactionInterval(long emptyTransactionInterval) {
        this.emptyTransactionInterval = emptyTransactionInterval;
    }

    public void setEmptyTransctionThresold(long emptyTransctionThresold) {
        this.emptyTransctionThresold = emptyTransctionThresold;
    }

}
