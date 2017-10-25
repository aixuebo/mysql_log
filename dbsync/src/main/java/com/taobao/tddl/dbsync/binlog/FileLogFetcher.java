package com.taobao.tddl.dbsync.binlog;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;

/**
 * TODO: Document It!!
 * 
 * <pre>
 * FileLogFetcher fetcher = new FileLogFetcher();
 * fetcher.open(file, 0);
 * 
 * while (fetcher.fetch()) {
 *     LogEvent event;
 *     do {
 *         event = decoder.decode(fetcher, context);
 * 
 *         // process log event.
 *     } while (event != null);
 * }
 * // file ending reached.
 * </pre>
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 * 读取文件的方式 进行binlog抓去
 */
public final class FileLogFetcher extends LogFetcher {

    public static final byte[] BINLOG_MAGIC = { -2, 0x62, 0x69, 0x6e };

    private FileInputStream    fin;//binlog文件的输入流

    public FileLogFetcher(){
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public FileLogFetcher(final int initialCapacity){
        super(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public FileLogFetcher(final int initialCapacity, final float growthFactor){
        super(initialCapacity, growthFactor);
    }

    /**
     * Open binlog file in local disk to fetch.
     */
    public void open(File file) throws FileNotFoundException, IOException {
        open(file, 0L);
    }

    /**
     * Open binlog file in local disk to fetch.
     */
    public void open(String filePath) throws FileNotFoundException, IOException {
        open(new File(filePath), 0L);
    }

    /**
     * Open binlog file in local disk to fetch.
     * 在本地磁盘上打开一个binlog文件去抓去数据
     */
    public void open(String filePath, final long filePosition) throws FileNotFoundException, IOException {
        open(new File(filePath), filePosition);
    }

    /**
     * Open binlog file in local disk to fetch.
     * 在本地磁盘上打开一个binlog文件去抓去数据
     */
    public void open(File file, final long filePosition) throws FileNotFoundException, IOException {
        fin = new FileInputStream(file);

        ensureCapacity(BIN_LOG_HEADER_SIZE);//确保有4个字节头文件
        //从文件中中读取4个字节,存放到buffer中,长度必须是4个字节,否则抛异常
        if (BIN_LOG_HEADER_SIZE != fin.read(buffer, 0, BIN_LOG_HEADER_SIZE)) throw new IOException("No binlog file header");

        //判断四个字节的内容符合标准
        if (buffer[0] != BINLOG_MAGIC[0] || buffer[1] != BINLOG_MAGIC[1] || buffer[2] != BINLOG_MAGIC[2]
            || buffer[3] != BINLOG_MAGIC[3]) {
            throw new IOException("Error binlog file header: "
                                  + Arrays.toString(Arrays.copyOf(buffer, BIN_LOG_HEADER_SIZE)));
        }

        //初始化文件buffer缓冲区
        limit = 0;//表示文件已经写到buffer的什么字节位置了
        origin = 0;
        position = 0;

        if (filePosition > BIN_LOG_HEADER_SIZE) {//定位到指定文件流位置
        	//读取文件开始部分,关于描述信息的内容
            /**
             最多的头文件字节数:
             公共头 19
             version(2)+描述(50)+头的字节数(4)
             事件占用的字节数(164)
             校验和4 + 校验和算法1
             */
            final int maxFormatDescriptionEventLen = FormatDescriptionLogEvent.LOG_EVENT_MINIMAL_HEADER_LEN //公共头
                                                     + FormatDescriptionLogEvent.ST_COMMON_HEADER_LEN_OFFSET //额外的头,表示binlog的版本,以及服务器的版本描述
                                                     + LogEvent.ENUM_END_EVENT //事件数量
                                                     + LogEvent.BINLOG_CHECKSUM_ALG_DESC_LEN //4个字节,表示校验和的长度
                                                     + LogEvent.CHECKSUM_CRC32_SIGNATURE_LEN;//校验和算法,1个字节

            ensureCapacity(maxFormatDescriptionEventLen);//buffer扩容
            limit = fin.read(buffer, 0, maxFormatDescriptionEventLen);//读取数据maxFormatDescriptionEventLen个字节,读取到buffer中,从buffer的0位置开始覆盖
            limit = (int) getUint32(LogEvent.EVENT_LEN_OFFSET);//表示一共多少个字节是有效的---即在第9个字节位置上读取四个字节,组成的int
            fin.getChannel().position(filePosition);//定位到文件流的位置
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#fetch()
     * true表示抓去成功,文件中还有字节可以去解析
     */
    public boolean fetch() throws IOException {
        if (limit == 0) {//表示没有有效内容了,则从文件中读取缓冲区个字节数据
            final int len = fin.read(buffer, 0, buffer.length);//返回真实读取的字节长度
            if (len >= 0) {
                limit += len;//有效字节数量
                position = 0;
                origin = 0;

                /* More binlog to fetch */
                return true;
            }
        } else if (origin == 0) {//起初读取文件
            if (limit > buffer.length / 2) {//扩容
                ensureCapacity(buffer.length + limit);
            }
            //从文件中读取buffer.length - limit个字节,即将buffer剩余的空间都读取成有效的字节,从buffer的limit位置开始覆盖
            final int len = fin.read(buffer, limit, buffer.length - limit);//返回真正读取的字节数量
            if (len >= 0) {
                limit += len;//增加limit有效字节数量

                /* More binlog to fetch 说明有更多的日志被读取了,因此返回*/
                return true;
            }
        } else if (limit > 0) {//说明此时limit有效字节还是有一定数量的
            System.arraycopy(buffer, origin, buffer, 0, limit);//从buffer中的origin位置开始拷贝所有有效字节数,从0位置开始覆盖,即整理一下buffer的空间,因为不是环形的,所以需要这样整理
            position -= origin;//位置就向前移动origin个,很好理解,因为基数位置要被调成0了,所以position位置也会变化
            origin = 0;//因此基数位置就是0了
            //然后从文件中读取数据buffer.length - limit个,即读取剩余buffer空间可以存放的字节数,从有效字节位置开始覆盖
            final int len = fin.read(buffer, limit, buffer.length - limit);
            if (len >= 0) {
                limit += len;//增加有效字节数

                /* More binlog to fetch */
                return true;
            }
        } else {
            /* Should not happen. */
            throw new IllegalArgumentException("Unexcepted limit: " + limit);
        }

        /* Reach binlog file end */
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#close()
     */
    public void close() throws IOException {
        if (fin != null) fin.close();

        fin = null;
    }
}
