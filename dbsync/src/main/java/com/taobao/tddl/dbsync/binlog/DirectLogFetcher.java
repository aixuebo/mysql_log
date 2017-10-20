package com.taobao.tddl.dbsync.binlog;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * TODO: Document It!!
 * 
 * <pre>
 * DirectLogFetcher fetcher = new DirectLogFetcher();
 * fetcher.open(conn, file, 0, 13);
 * 
 * while (fetcher.fetch()) {
 *     LogEvent event;
 *     do {
 *         event = decoder.decode(fetcher, context);
 * 
 *         // process log event.
 *     } while (event != null);
 * }
 * // connection closed.
 * </pre>
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 * 通过JDBC等直接方式,去连接mysql,获取binlog等信息,而不是直接读取binlog文件了
 */
public final class DirectLogFetcher extends LogFetcher {

    protected static final Log logger                          = LogFactory.getLog(DirectLogFetcher.class);

    /** Command to dump binlog */
    public static final byte   COM_BINLOG_DUMP                 = 18;//设置binlog的信息

    /** Packet header sizes */
    public static final int    NET_HEADER_SIZE                 = 4;//包头文件的大小
    public static final int    SQLSTATE_LENGTH                 = 5;

    /** Packet offsets 头文件的位置*/
    public static final int    PACKET_LEN_OFFSET               = 0;//body大小的位置
    public static final int    PACKET_SEQ_OFFSET               = 3;//记录包序号的位置

    /** Maximum packet length 一个包最大的长度*/
    public static final int    MAX_PACKET_LENGTH               = (256 * 256 * 256 - 1);

    /** BINLOG_DUMP options */
    public static final int    BINLOG_DUMP_NON_BLOCK           = 1;
    public static final int    BINLOG_SEND_ANNOTATE_ROWS_EVENT = 2;

    private Connection         conn;//连接器
    private OutputStream       mysqlOutput;//mysql的输出信息
    private InputStream        mysqlInput;//mysql的输入流

    public DirectLogFetcher(){
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity){
        super(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity, final float growthFactor){
        super(initialCapacity, growthFactor);
    }

    /**
     * 类型转换,实例类conn一定以下类的一个子类
     * connClazz
     * org.springframework.jdbc.datasource.ConnectionProxy,那么就是执行getTargetConnection方法
     * org.apache.commons.dbcp.DelegatingConnection,那么就是执行_conn属性
     * com.mysql.jdbc.Connection
     */
    private static final Object unwrapConnection(Object conn, Class<?> connClazz) throws IOException {
        while (!connClazz.isInstance(conn)) {
            try {
                Class<?> connProxy = Class.forName("org.springframework.jdbc.datasource.ConnectionProxy");
                if (connProxy.isInstance(conn)) {
                    conn = invokeMethod(conn, connProxy, "getTargetConnection");
                    continue;
                }
            } catch (ClassNotFoundException e) {
                // org.springframework.jdbc.datasource.ConnectionProxy not
                // found.
            }

            try {
                Class<?> connProxy = Class.forName("org.apache.commons.dbcp.DelegatingConnection");
                if (connProxy.isInstance(conn)) {
                    conn = getDeclaredField(conn, connProxy, "_conn");
                    continue;
                }
            } catch (ClassNotFoundException e) {
                // org.apache.commons.dbcp.DelegatingConnection not found.
            }

            try {
                if (conn instanceof java.sql.Wrapper) {
                    Class<?> connIface = Class.forName("com.mysql.jdbc.Connection");
                    conn = ((java.sql.Wrapper) conn).unwrap(connIface);
                    continue;
                }
            } catch (ClassNotFoundException e) {
                // com.mysql.jdbc.Connection not found.
            } catch (SQLException e) {
                logger.warn("Unwrap " + conn.getClass().getName() + " to " + connClazz.getName() + " failed: "
                            + e.getMessage(),
                    e);
            }

            return null;
        }
        return conn;
    }

    //执行该class的没有参数的name方法,返回object对象
    private static final Object invokeMethod(Object obj, Class<?> objClazz, String name) {
        try {
            Method method = objClazz.getMethod(name, (Class<?>[]) null);
            return method.invoke(obj, (Object[]) null);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("No such method: \'" + name + "\' @ " + objClazz.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot invoke method: \'" + name + "\' @ " + objClazz.getName(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Invoke method failed: \'" + name + "\' @ " + objClazz.getName(),
                e.getTargetException());
        }
    }

    //获取objClazz的name属性对应的对象Object
    private static final Object getDeclaredField(Object obj, Class<?> objClazz, String name) {
        try {
            Field field = objClazz.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(obj);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("No such field: \'" + name + "\' @ " + objClazz.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot get field: \'" + name + "\' @ " + objClazz.getName(), e);
        }
    }

    /**
     * Connect MySQL master to fetch binlog.
     */
    public void open(Connection conn, String fileName, final int serverId) throws IOException {
        open(conn, fileName, BIN_LOG_HEADER_SIZE, serverId, false);
    }

    /**
     * Connect MySQL master to fetch binlog.
     */
    public void open(Connection conn, String fileName, final int serverId, boolean nonBlocking) throws IOException {
        open(conn, fileName, BIN_LOG_HEADER_SIZE, serverId, nonBlocking);
    }

    /**
     * Connect MySQL master to fetch binlog.
     */
    public void open(Connection conn, String fileName, final long filePosition, final int serverId) throws IOException {
        open(conn, fileName, filePosition, serverId, false);
    }

    /**
     * Connect MySQL master to fetch binlog.
     */
    public void open(Connection conn, String fileName, long filePosition, final int serverId, boolean nonBlocking)
                                                                                                                  throws IOException {
        try {
            this.conn = conn;
            Class<?> connClazz = Class.forName("com.mysql.jdbc.ConnectionImpl");
            Object unwrapConn = unwrapConnection(conn, connClazz);
            if (unwrapConn == null) {
                throw new IOException("Unable to unwrap " + conn.getClass().getName()
                                      + " to com.mysql.jdbc.ConnectionImpl");
            }

            // Get underlying IO streams for network communications.
            Object connIo = getDeclaredField(unwrapConn, connClazz, "io");
            if (connIo == null) {
                throw new IOException("Get null field:" + conn.getClass().getName() + "#io");
            }
            mysqlOutput = (OutputStream) getDeclaredField(connIo, connIo.getClass(), "mysqlOutput");
            mysqlInput = (InputStream) getDeclaredField(connIo, connIo.getClass(), "mysqlInput");

            if (filePosition == 0) filePosition = BIN_LOG_HEADER_SIZE;
            sendBinlogDump(fileName, filePosition, serverId, nonBlocking);
            position = 0;
        } catch (IOException e) {
            close(); /* Do cleanup */
            logger.error("Error on COM_BINLOG_DUMP: file = " + fileName + ", position = " + filePosition);
            throw e;
        } catch (ClassNotFoundException e) {
            close(); /* Do cleanup */
            throw new IOException("Unable to load com.mysql.jdbc.ConnectionImpl", e);
        }
    }

    /**
     * Put a byte in the buffer.
     * 
     * @param b the byte to put in the buffer
     * buffer中存放1个byte
     */
    protected final void putByte(byte b) {
        ensureCapacity(position + 1);

        buffer[position++] = b;
    }

    /**
     * Put 16-bit integer in the buffer.
     * 
     * @param i16 the integer to put in the buffer
     * buffer中存放2个byte,该2个byte表示int
     */
    protected final void putInt16(int i16) {
        ensureCapacity(position + 2);

        byte[] buf = buffer;
        buf[position++] = (byte) (i16 & 0xff);
        buf[position++] = (byte) (i16 >>> 8);
    }

    /**
     * Put 32-bit integer in the buffer.
     * 
     * @param i32 the integer to put in the buffer
     * buffer中存放4个int,该4个byte表示long
     */
    protected final void putInt32(long i32) {
        ensureCapacity(position + 4);

        byte[] buf = buffer;
        buf[position++] = (byte) (i32 & 0xff);
        buf[position++] = (byte) (i32 >>> 8);
        buf[position++] = (byte) (i32 >>> 16);
        buf[position++] = (byte) (i32 >>> 24);
    }

    /**
     * Put a string in the buffer.
     * 
     * @param s the value to put in the buffer
     * buffer中存放String个字节
     */
    protected final void putString(String s) {
        ensureCapacity(position + (s.length() * 2) + 1);

        System.arraycopy(s.getBytes(), 0, buffer, position, s.length());
        position += s.length();
        buffer[position++] = 0;//存储完string后,加一个0的字节内容,表示是string结束了
    }

    //设置binlog的一些信息,比如要读取哪个binlog文件,是什么slave读取的该文件等信息
    protected final void sendBinlogDump(String fileName, final long filePosition, final int serverId,
                                        boolean nonBlocking) throws IOException {
        position = NET_HEADER_SIZE;

        putByte(COM_BINLOG_DUMP);//设置binlog的一些信息
        putInt32(filePosition);
        int binlog_flags = nonBlocking ? BINLOG_DUMP_NON_BLOCK : 0;
        binlog_flags |= BINLOG_SEND_ANNOTATE_ROWS_EVENT;
        putInt16(binlog_flags); // binlog_flags
        putInt32(serverId); // slave's server-id
        putString(fileName);

        final byte[] buf = buffer;
        final int len = position - NET_HEADER_SIZE;
        buf[0] = (byte) (len & 0xff);
        buf[1] = (byte) (len >>> 8);
        buf[2] = (byte) (len >>> 16);

        mysqlOutput.write(buffer, 0, position);//从buffer中第0个位置开始读取,读取position个字节,写入到输出流中
        mysqlOutput.flush();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#fetch()
     */
    public boolean fetch() throws IOException {
        try {
            // Fetching packet header from input.
        	//先读取4个字节的头文件
            if (!fetch0(0, NET_HEADER_SIZE)) {//false说明没有读全就没有数据了,显然是不对的,因此返回false,退出抓去
                logger.warn("Reached end of input stream while fetching header");
                return false;
            }

            // Fetching the first packet(may a multi-packet).
            //读取body大小和bao序号
            int netlen = getUint24(PACKET_LEN_OFFSET);
            int netnum = getUint8(PACKET_SEQ_OFFSET);
            if (!fetch0(NET_HEADER_SIZE, netlen)) {//抓去body大小的内容,并且内容追加在第5个字节位置
                logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                return false;
            }

            // Detecting error code.发现错误编码
            final int mark = getUint8(NET_HEADER_SIZE);//获取拿到的body内容
            if (mark != 0) {//正常情况下,返回的编码应该是0,不是0,说明有异常
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
                    logger.warn("Received EOF packet from server, apparent" + " master disconnected.");
                    return false;
                } else {
                    // Should not happen.
                    throw new IOException("Unexpected response " + mark + " while fetching binlog: packet #" + netnum
                                          + ", len = " + netlen);
                }
            }

            // The first packet is a multi-packet, concatenate the packets.
            while (netlen == MAX_PACKET_LENGTH) {//等于最大包文件大小,因此说明是多个包组成的一个包
                if (!fetch0(0, NET_HEADER_SIZE)) {//抓包头文件
                    logger.warn("Reached end of input stream while fetching header");
                    return false;
                }

                netlen = getUint24(PACKET_LEN_OFFSET);
                netnum = getUint8(PACKET_SEQ_OFFSET);
                if (!fetch0(limit, netlen)) {//抓body文件
                    logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                    return false;
                }
            }

            // Preparing buffer variables to decoding.
            //跳过包的头文件位置
            origin = NET_HEADER_SIZE + 1;
            position = origin;
            limit -= origin;
            return true;
        } catch (SocketTimeoutException e) {
            close(); /* Do cleanup */
            logger.error("Socket timeout expired, closing connection", e);
            throw e;
        } catch (InterruptedIOException e) {
            close(); /* Do cleanup */
            logger.warn("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (IOException e) {
            close(); /* Do cleanup */
            logger.error("I/O error while reading from client socket", e);
            throw e;
        }
    }

    //需要读取len个字节,true表示成功抓去了len个字节
    private final boolean fetch0(final int off, final int len) throws IOException {
        ensureCapacity(off + len);

        //count表示每一次读取的信息字节数,n表示多次循环后总共读取的字节数,len表示需要读取的字节数
        for (int count, n = 0; n < len; n += count) {
        	//每次循环n读取的总字节数都会有变化,但是off一直没变化,因此off+n就是要插入buffer的开始位置,从流中读取的最大长度就是len-已经读取的n个,即剩余字节数
            if (0 > (count = mysqlInput.read(buffer, off + n, len - n))) {//说明没有信息了,因此返回0>-1,因此最终返回false
                // Reached end of input stream 读取到数据流的结尾了
                return false;
            }
        }

        //因为在off之后插入了len个字节,因此有效字节就是off+len个,其实这句话有点看不懂,按道理应该有效字节是limit+len个才对,不过这个也说明是按照程序的逻辑,强依赖off的,代码必须严谨
        if (limit < off + len) limit = off + len;
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#close()
     * 关闭连接流
     */
    public void close() throws IOException {
        try {
            if (conn != null) conn.close();

            conn = null;
            mysqlInput = null;
            mysqlOutput = null;
        } catch (SQLException e) {
            logger.warn("Unable to close connection", e);
        }
    }
}
