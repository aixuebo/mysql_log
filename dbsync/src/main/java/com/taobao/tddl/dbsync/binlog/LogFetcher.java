package com.taobao.tddl.dbsync.binlog;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * Declaration a binary-log fetcher. It extends from <code>LogBuffer</code>.
 * 
 * <pre>
 * LogFetcher fetcher = new SomeLogFetcher();
 * ...
 * 
 * while (fetcher.fetch())//不断的读取有效字节,并且将有效字节存储到buffer缓冲中
 * {
 *     LogEvent event;
 *     do
 *     {
 *         event = decoder.decode(fetcher, context);//对有效字节进行转换成对应的事件
 * 
 *         // process log event.
 *     }
 *     while (event != null);
 * }
 * // no more binlog.
 * fetcher.close();
 * </pre>
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public abstract class LogFetcher extends LogBuffer implements Closeable {

    /** Default initial capacity. 默认初始化容量*/
    public static final int   DEFAULT_INITIAL_CAPACITY = 8192;

    /** Default growth factor. */
    public static final float DEFAULT_GROWTH_FACTOR    = 2.0f;//默认扩容因子

    /** Binlog file header size 二进制文件头大小*/
    public static final int   BIN_LOG_HEADER_SIZE      = 4;

    protected final float     factor;//扩容因子

    public LogFetcher(){
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public LogFetcher(final int initialCapacity){
        this(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public LogFetcher(final int initialCapacity, final float growthFactor){
        this.buffer = new byte[initialCapacity];
        this.factor = growthFactor;
    }

    /**
     * Increases the capacity of this <tt>LogFetcher</tt> instance, if
     * necessary, to ensure that it can hold at least the number of elements
     * specified by the minimum capacity argument.
     * 
     * @param minCapacity the desired minimum capacity
     * 确保扩容达到minCapacity的字节长度
     */
    protected final void ensureCapacity(final int minCapacity) {
        final int oldCapacity = buffer.length;//现有长度

        if (minCapacity > oldCapacity) {//长度不足
            int newCapacity = (int) (oldCapacity * factor);//扩容
            if (newCapacity < minCapacity) newCapacity = minCapacity;//扩容后还是不满足,则直接使用参数minCapacity作为目标字节数

            buffer = Arrays.copyOf(buffer, newCapacity);
        }
    }

    /**
     * Fetches the next frame of binary-log, and fill it in buffer.
     * true表示依然存在有效的字节需要被处理,并且已经将有效字节填充到buffer中
     */
    public abstract boolean fetch() throws IOException;

    /**
     * {@inheritDoc}
     * 
     * @see java.io.Closeable#close()
     * 关闭输入源文件流
     */
    public abstract void close() throws IOException;
}
