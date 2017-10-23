package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tddl.dbsync.binlog.LogFetcher;

/**
 * 基于socket的logEvent实现
 * 
 * @author jianghang 2013-1-14 下午07:39:30
 * @version 1.0.0
 * 直接抓去master的response日志
 */
public class DirectLogFetcher extends LogFetcher {

    protected static final Logger logger            = LoggerFactory.getLogger(DirectLogFetcher.class);

    /** Command to dump binlog */
    public static final byte      COM_BINLOG_DUMP   = 18;

    /** Packet header sizes */
    public static final int       NET_HEADER_SIZE   = 4;
    public static final int       SQLSTATE_LENGTH   = 5;

    /** Packet offsets */
    public static final int       PACKET_LEN_OFFSET = 0;//文件长度的位置
    public static final int       PACKET_SEQ_OFFSET = 3;//获取包序号的位置

    /** Maximum packet length */
    public static final int       MAX_PACKET_LENGTH = (256 * 256 * 256 - 1);//最大包的长度

    private SocketChannel         channel;//该channel已经连接了master

    // private BufferedInputStream input;

    public DirectLogFetcher(){
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity){
        super(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity, final float growthFactor){
        super(initialCapacity, growthFactor);
    }

    public void start(SocketChannel channel) throws IOException {
        this.channel = channel;
        // 和mysql driver一样，提供buffer机制，提升读取binlog速度
        // this.input = new
        // BufferedInputStream(channel.socket().getInputStream(), 16384);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#fetch()
     * 一次抓去后,buff的内容就是所有的返回值
     */
    public boolean fetch() throws IOException {
        try {
            // Fetching packet header from input.
            if (!fetch0(0, NET_HEADER_SIZE)) {//抓去4个数据头文件
                logger.warn("Reached end of input stream while fetching header");
                return false;
            }

            // Fetching the first packet(may a multi-packet).
            int netlen = getUint24(PACKET_LEN_OFFSET);//返回response的文件长度
            int netnum = getUint8(PACKET_SEQ_OFFSET);//以及返回文件包的序号
            if (!fetch0(NET_HEADER_SIZE, netlen)) {//从4位置开始缓存buffer数据,抓去response的长度信息
                logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                return false;
            }

            // Detecting error code.
            final int mark = getUint8(NET_HEADER_SIZE);//获取错误标识
            if (mark != 0) {
                if (mark == 255) // error from master
                {
                    // Indicates an error, for example trying to fetch from
                    // wrong
                    // binlog position.
                    position = NET_HEADER_SIZE + 1;
                    final int errno = getInt16();
                    String sqlstate = forward(1).getFixString(SQLSTATE_LENGTH);
                    String errmsg = getFixString(limit - position);
                    throw new IOException("Received error packet:" + " errno = " + errno + ", sqlstate = " + sqlstate
                                          + " errmsg = " + errmsg);
                } else if (mark == 254) {
                    // Indicates end of stream. It's not clear when this would
                    // be sent.
                    logger.warn("Received EOF packet from server, apparent"
                                + " master disconnected. It's may be duplicate slaveId , check instance config");
                    return false;
                } else {
                    // Should not happen.
                    throw new IOException("Unexpected response " + mark + " while fetching binlog: packet #" + netnum
                                          + ", len = " + netlen);
                }
            }

            // The first packet is a multi-packet, concatenate the packets.
            while (netlen == MAX_PACKET_LENGTH) {//说明包慢着呢
                if (!fetch0(0, NET_HEADER_SIZE)) {
                    logger.warn("Reached end of input stream while fetching header");
                    return false;
                }

                netlen = getUint24(PACKET_LEN_OFFSET);
                netnum = getUint8(PACKET_SEQ_OFFSET);
                if (!fetch0(limit, netlen)) {
                    logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                    return false;
                }
            }

            // Preparing buffer variables to decoding.
            origin = NET_HEADER_SIZE + 1;//buffer的开始位置
            position = origin;//当前处理到哪个位置了
            limit -= origin;//总长度,即origin位置的信息可以丢弃了
            return true;
        } catch (SocketTimeoutException e) {
            close(); /* Do cleanup */
            logger.error("Socket timeout expired, closing connection", e);
            throw e;
        } catch (InterruptedIOException e) {
            close(); /* Do cleanup */
            logger.info("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (ClosedByInterruptException e) {
            close(); /* Do cleanup */
            logger.info("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (IOException e) {
            close(); /* Do cleanup */
            logger.error("I/O error while reading from client socket", e);
            throw e;
        }
    }

    //读取len个数据,将其存储到buff的off位置上进行缓存
    private final boolean fetch0(final int off, final int len) throws IOException {
        ensureCapacity(off + len);

        ByteBuffer buffer = ByteBuffer.wrap(this.buffer, off, len);
        while (buffer.hasRemaining()) {//读取数据
            int readNum = channel.read(buffer);
            if (readNum == -1) {
                throw new IOException("Unexpected End Stream");
            }
        }

        // for (int count, n = 0; n < len; n += count) {
        // if (0 > (count = input.read(buffer, off + n, len - n))) {
        // // Reached end of input stream
        // return false;
        // }
        // }

        if (limit < off + len) limit = off + len;//更新buff的limit位置
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#close()
     */
    public void close() throws IOException {
        // do nothing
    }

}
