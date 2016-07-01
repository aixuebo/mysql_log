package com.taobao.tddl.dbsync.binlog;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.BitSet;

/**
 * TODO: Document Me!!
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public class LogBuffer {

    protected byte[] buffer;//字节缓冲池

    //注意origin limit position三个变量其实都是绝对位置,不是某一个文件的相对位置
    protected int    origin, limit;//origin表示当前的基础位置,limit表示文件已经写到buffer的有效字节数
    protected int    position;//当前操作到哪个位置了,该位置应该是origin<position<origin+limit

    protected LogBuffer(){
    }

    //初始化一个缓冲池
    public LogBuffer(byte[] buffer, final int origin, final int limit){
        if (origin + limit > buffer.length) throw new IllegalArgumentException("capacity excceed: " + (origin + limit));

        this.buffer = buffer;
        this.origin = origin;
        this.position = origin;
        this.limit = limit;
    }

    /**
     * Return n bytes in this buffer.
     * 复制buffer的内容,产生新的LogBuffer对象,老的对象依然存在,不变化。
     * 新的LogBuffer对象是从buffer的origin + pos位置开始读取.读取len个长度组成的
     */
    public final LogBuffer duplicate(final int pos, final int len) {
        if (pos + len > limit) throw new IllegalArgumentException("limit excceed: " + (pos + len));

        // XXX: Do momery copy avoid buffer modified.
        final int off = origin + pos;
        byte[] buf = Arrays.copyOfRange(buffer, off, off + len);
        return new LogBuffer(buf, 0, len);
    }

    /**
     * Return next n bytes in this buffer.
     * 复制buffer的内容,产生新的LogBuffer对象,老的对象依然存在,但是少有变化,因为position位置也会跟随变化
     * 新的LogBuffer对象是从buffer的position位置开始读取.读取len个长度组成的
     * 并且position位置也会跟随变化
     */
    public final LogBuffer duplicate(final int len) {
        if (position + len > origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                                + (position + len - origin));

        // XXX: Do momery copy avoid buffer modified.
        final int end = position + len;
        byte[] buf = Arrays.copyOfRange(buffer, position, end);
        LogBuffer dupBuffer = new LogBuffer(buf, 0, len);
        position = end;
        return dupBuffer;
    }

    /**
     * Return next n bytes in this buffer.
     * 赋值全部buffer的内容,产生新的LogBuffer对象,老的对象依然存在,并且没有变化
     */
    public final LogBuffer duplicate() {
        // XXX: Do momery copy avoid buffer modified.
        byte[] buf = Arrays.copyOfRange(buffer, origin, origin + limit);
        return new LogBuffer(buf, 0, limit);
    }

    /**
     * Returns this buffer's capacity. </p>
     * 
     * @return The capacity of this buffer
     * 返回buffer缓冲数组中的容量
     */
    public final int capacity() {
        return buffer.length;
    }

    /**
     * Returns this buffer's position. </p>
     * 
     * @return The position of this buffer
     * 该位置是在当前缓冲池中的位置,即属于相对位置
     */
    public final int position() {
        return position - origin;
    }

    /**
     * Sets this buffer's position. If the mark is defined and larger than the
     * new position then it is discarded. </p>
     * 
     * @param newPosition The new position value; must be non-negative and no
     * larger than the current limit
     * @return This buffer
     * @throws IllegalArgumentException If the preconditions on
     * <tt>newPosition</tt> do not hold
     * 重新定义位置,该位置参数是相对位置
     */
    public final LogBuffer position(final int newPosition) {
        if (newPosition > limit || newPosition < 0) throw new IllegalArgumentException("limit excceed: " + newPosition);

        this.position = origin + newPosition;
        return this;
    }

    /**
     * Forwards this buffer's position.
     * 
     * @param len The forward distance
     * @return This buffer
     * 将position移动len个位置
     */
    public final LogBuffer forward(final int len) {
        if (position + len > origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                                + (position + len - origin));

        this.position += len;
        return this;
    }

    /**
     * Consume this buffer, moving origin and position.
     * 
     * @param len The consume distance
     * @return This buffer
     * 消费N个字节,因此buffer的长度就会减少N个字节
     * 1.
     */
    public final LogBuffer consume(final int len) {
        if (limit > len) {//说明总长度已经超过了等待消费的字节数,
            limit -= len;//总长度减少
            origin += len;//起始位置前移动.因为这部分数据已经被消费掉了,没意义了,后续需要字节空间的可以用这部分空间
            position = origin;//位置重新移动到原始位置上
            return this;
        } else if (limit == len) {//说明全部要被消费掉
            limit = 0;
            origin = 0;
            position = 0;
            return this;
        } else {
            /* Should not happen. */
            throw new IllegalArgumentException("limit excceed: " + len);
        }
    }

    /**
     * Rewinds this buffer. The position is set to zero.
     * 
     * @return This buffer
     * 表示从头开始读取数据,即将position当前位置设置成origin基础位置
     */
    public final LogBuffer rewind() {
        position = origin;
        return this;
    }

    /**
     * Returns this buffer's limit. </p>
     * 
     * @return The limit of this buffer
     * 返回当前buffer的最后字节位置
     */
    public final int limit() {
        return limit;
    }

    /**
     * Sets this buffer's limit. If the position is larger than the new limit
     * then it is set to the new limit. If the mark is defined and larger than
     * the new limit then it is discarded. </p>
     * 
     * @param newLimit The new limit value; must be non-negative and no larger
     * than this buffer's capacity
     * @return This buffer
     * @throws IllegalArgumentException If the preconditions on <tt>newLimit</tt>
     * do not hold
     * 拦腰截断buffer内容.将buffer有效的内容最后位置重新设置成newLimit
     */
    public final LogBuffer limit(int newLimit) {
        if (origin + newLimit > buffer.length || newLimit < 0) throw new IllegalArgumentException("capacity excceed: "
                                                                                                  + (origin + newLimit));

        limit = newLimit;
        return this;
    }

    /**
     * Returns the number of elements between the current position and the
     * limit. </p>
     * 
     * @return The number of elements remaining in this buffer
     * 表示还剩余多少个字节
     */
    public final int remaining() {
        return limit + origin - position;
    }

    /**
     * Tells whether there are any elements between the current position and the
     * limit. </p>
     * 
     * @return <tt>true</tt> if, and only if, there is at least one element
     * remaining in this buffer
     * true表示还有字节没有读取完
     */
    public final boolean hasRemaining() {
        return position < limit + origin;
    }

    //--------以下代码就是如何从buffer中获取各种int long strring的数据方法了-----------------------
    /**
     * Return 8-bit signed int from buffer.
     * 在当前文件的位置 前进pos,读取一个byte,转换成int
     */
    public final int getInt8(final int pos) {
        if (pos >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: " + pos);

        return buffer[origin + pos];
    }

    /**
     * Return next 8-bit signed int from buffer.
     * 读取一个byte,转换成int
     */
    public final int getInt8() {
        if (position >= origin + limit) throw new IllegalArgumentException("limit excceed: " + (position - origin));

        return buffer[position++];
    }

    /**
     * Return 8-bit unsigned int from buffer.
     * 读取一个byte,转换成int
     */
    public final int getUint8(final int pos) {
    	//origin + pos表示从当前位置前进几个字节,到达下一个什么位置
        if (pos >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: " + pos);

        return 0xff & buffer[origin + pos];
    }

    /**
     * Return next 8-bit unsigned int from buffer.
     * 读取下一个byte,转换成int
     */
    public final int getUint8() {
    	//位置不能超越范围
        if (position >= origin + limit) throw new IllegalArgumentException("limit excceed: " + (position - origin));

        return 0xff & buffer[position++];
    }

    /**
     * Return 16-bit signed int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint2korr
     * 在当前文件的位置 前进pos,读取接下来2个字节,返回int
     */
    public final int getInt16(final int pos) {
        final int position = origin + pos;

        if (pos + 1 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 1)));

        byte[] buf = buffer;
        return (0xff & buf[position]) | ((buf[position + 1]) << 8);
    }

    /**
     * Return next 16-bit signed int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint2korr
     * 读取接下来2个字节,返回int
     */
    public final int getInt16() {
        if (position + 1 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 1));

        byte[] buf = buffer;
        return (0xff & buf[position++]) | ((buf[position++]) << 8);
    }

    /**
     * Return 16-bit unsigned int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint2korr
     * 在当前文件的位置 前进pos,读取接下来2个字节,返回int
     */
    public final int getUint16(final int pos) {
        final int position = origin + pos;

        if (pos + 1 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 1)));

        byte[] buf = buffer;
        return (0xff & buf[position]) | ((0xff & buf[position + 1]) << 8);
    }

    /**
     * Return next 16-bit unsigned int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint2korr
     * 读取接下来2个字节,返回int
     */
    public final int getUint16() {
        if (position + 1 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 1));

        byte[] buf = buffer;
        return (0xff & buf[position++]) | ((0xff & buf[position++]) << 8);
    }

    /**
     * Return 16-bit signed int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_sint2korr
     * 在当前文件的位置 前进pos,读取接下来2个字节,返回int
     */
    public final int getBeInt16(final int pos) {
        final int position = origin + pos;

        if (pos + 1 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 1)));

        byte[] buf = buffer;
        return (0xff & buf[position + 1]) | ((buf[position]) << 8);
    }

    /**
     * Return next 16-bit signed int from buffer. (big-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - mi_sint2korr
     * 读取接下来2个字节,返回int
     */
    public final int getBeInt16() {
        if (position + 1 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 1));

        byte[] buf = buffer;
        return (buf[position++] << 8) | (0xff & buf[position++]);
    }

    /**
     * Return 16-bit unsigned int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_usint2korr
     * 在当前文件的位置 前进pos,读取接下来2个字节,返回int
     */
    public final int getBeUint16(final int pos) {
        final int position = origin + pos;

        if (pos + 1 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 1)));

        byte[] buf = buffer;
        return (0xff & buf[position + 1]) | ((0xff & buf[position]) << 8);
    }

    /**
     * Return next 16-bit unsigned int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_usint2korr
     * 读取接下来2个字节,返回int
     */
    public final int getBeUint16() {
        if (position + 1 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 1));

        byte[] buf = buffer;
        return ((0xff & buf[position++]) << 8) | (0xff & buf[position++]);
    }

    /**
     * Return 24-bit signed int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint3korr
     * 在当前文件的位置 前进pos,读取接下来3个字节,返回int
     */
    public final int getInt24(final int pos) {
        final int position = origin + pos;

        if (pos + 2 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 2)));

        byte[] buf = buffer;
        return (0xff & buf[position]) | ((0xff & buf[position + 1]) << 8) | ((buf[position + 2]) << 16);
    }

    /**
     * Return next 24-bit signed int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint3korr
     * 读取接下来3个字节,返回int
     */
    public final int getInt24() {
        if (position + 2 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 2));

        byte[] buf = buffer;
        return (0xff & buf[position++]) | ((0xff & buf[position++]) << 8) | ((buf[position++]) << 16);
    }

    /**
     * Return 24-bit signed int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_usint3korr
     * 在当前文件的位置 前进pos,读取接下来3个字节,返回int
     */
    public final int getBeInt24(final int pos) {
        final int position = origin + pos;

        if (pos + 2 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 2)));

        byte[] buf = buffer;
        return (0xff & buf[position + 2]) | ((0xff & buf[position + 1]) << 8) | ((buf[position]) << 16);
    }

    /**
     * Return next 24-bit signed int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_usint3korr
     * 读取接下来3个字节,返回int
     */
    public final int getBeInt24() {
        if (position + 2 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 2));

        byte[] buf = buffer;
        return ((buf[position++]) << 16) | ((0xff & buf[position++]) << 8) | (0xff & buf[position++]);
    }

    /**
     * Return 24-bit unsigned int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint3korr
     * 在当前文件的位置 前进pos,读取接下来3个字节,返回int
     */
    public final int getUint24(final int pos) {
        final int position = origin + pos;

        if (pos + 2 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 2)));

        byte[] buf = buffer;
        return (0xff & buf[position]) | ((0xff & buf[position + 1]) << 8) | ((0xff & buf[position + 2]) << 16);
    }

    /**
     * Return next 24-bit unsigned int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint3korr
     * 读取接下来3个字节,返回int
     */
    public final int getUint24() {
        if (position + 2 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 2));

        byte[] buf = buffer;
        return (0xff & buf[position++]) | ((0xff & buf[position++]) << 8) | ((0xff & buf[position++]) << 16);
    }

    /**
     * Return 24-bit unsigned int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_usint3korr
     * 在当前文件的位置 前进pos,读取接下来3个字节,返回int
     */
    public final int getBeUint24(final int pos) {
        final int position = origin + pos;

        if (pos + 2 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 2)));

        byte[] buf = buffer;
        return (0xff & buf[position + 2]) | ((0xff & buf[position + 1]) << 8) | ((0xff & buf[position]) << 16);
    }

    /**
     * Return next 24-bit unsigned int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_usint3korr
     * 读取接下来3个字节,返回int
     */
    public final int getBeUint24() {
        if (position + 2 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 2));

        byte[] buf = buffer;
        return ((0xff & buf[position++]) << 16) | ((0xff & buf[position++]) << 8) | (0xff & buf[position++]);
    }

    /**
     * Return 32-bit signed int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint4korr
     * 在当前文件的位置 前进pos,读取接下来4个字节,返回int
     */
    public final int getInt32(final int pos) {
        final int position = origin + pos;

        if (pos + 3 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 3)));

        byte[] buf = buffer;
        return (0xff & buf[position]) | ((0xff & buf[position + 1]) << 8) | ((0xff & buf[position + 2]) << 16)
               | ((buf[position + 3]) << 24);
    }

    /**
     * Return 32-bit signed int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_sint4korr
     * 读取接下来4个字节,返回int
     */
    public final int getBeInt32(final int pos) {
        final int position = origin + pos;

        if (pos + 3 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 3)));

        byte[] buf = buffer;
        return (0xff & buf[position + 3]) | ((0xff & buf[position + 2]) << 8) | ((0xff & buf[position + 1]) << 16)
               | ((buf[position]) << 24);
    }

    /**
     * Return next 32-bit signed int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint4korr
     * 读取接下来4个字节,返回int
     */
    public final int getInt32() {
        if (position + 3 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 3));

        byte[] buf = buffer;
        return (0xff & buf[position++]) | ((0xff & buf[position++]) << 8) | ((0xff & buf[position++]) << 16)
               | ((buf[position++]) << 24);
    }

    /**
     * Return next 32-bit signed int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_sint4korr
     * 读取接下来4个字节,返回int
     */
    public final int getBeInt32() {
        if (position + 3 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 3));

        byte[] buf = buffer;
        return ((buf[position++]) << 24) | ((0xff & buf[position++]) << 16) | ((0xff & buf[position++]) << 8)
               | (0xff & buf[position++]);
    }

    /**
     * Return 32-bit unsigned int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint4korr
     * 在当前文件的位置 前进pos,读取接下来4个字节,返回int
     * 
     */
    public final long getUint32(final int pos) {
        final int position = origin + pos;//表示从当前位置前进几个字节,到达下一个什么位置

        if (pos + 3 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 3)));

        //从buffer中读取4个字节,组成int返回
        byte[] buf = buffer;
        return ((long) (0xff & buf[position])) | ((long) (0xff & buf[position + 1]) << 8)
               | ((long) (0xff & buf[position + 2]) << 16) | ((long) (0xff & buf[position + 3]) << 24);
    }

    /**
     * Return 32-bit unsigned int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_usint4korr
     * 在当前文件的位置 前进pos,读取接下来4个字节,返回int
     */
    public final long getBeUint32(final int pos) {
        final int position = origin + pos;

        if (pos + 3 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 3)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position + 3])) | ((long) (0xff & buf[position + 2]) << 8)
               | ((long) (0xff & buf[position + 1]) << 16) | ((long) (0xff & buf[position]) << 24);
    }

    /**
     * Return next 32-bit unsigned int from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint4korr
     * 读取接下来4个字节,返回int
     */
    public final long getUint32() {
        if (position + 3 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 3));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++])) | ((long) (0xff & buf[position++]) << 8)
               | ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 24);
    }

    /**
     * Return next 32-bit unsigned int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_uint4korr
     * 读取接下来4个字节,返回int
     */
    public final long getBeUint32() {
        if (position + 3 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 3));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++]) << 24) | ((long) (0xff & buf[position++]) << 16)
               | ((long) (0xff & buf[position++]) << 8) | ((long) (0xff & buf[position++]));
    }

    /**
     * Return 40-bit unsigned int from buffer. (little-endian)
     * 在当前文件的位置 前进pos,读取接下来5个字节,返回long
     */
    public final long getUlong40(final int pos) {
        final int position = origin + pos;

        if (pos + 4 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 4)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position])) | ((long) (0xff & buf[position + 1]) << 8)
               | ((long) (0xff & buf[position + 2]) << 16) | ((long) (0xff & buf[position + 3]) << 24)
               | ((long) (0xff & buf[position + 4]) << 32);
    }

    /**
     * Return next 40-bit unsigned int from buffer. (little-endian)
     * 读取接下来5个字节,返回long
     */
    public final long getUlong40() {
        if (position + 4 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 4));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++])) | ((long) (0xff & buf[position++]) << 8)
               | ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 24)
               | ((long) (0xff & buf[position++]) << 32);
    }

    /**
     * Return 40-bit unsigned int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_uint5korr
     */
    public final long getBeUlong40(final int pos) {
        final int position = origin + pos;

        if (pos + 4 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 4)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position + 4])) | ((long) (0xff & buf[position + 3]) << 8)
               | ((long) (0xff & buf[position + 2]) << 16) | ((long) (0xff & buf[position + 1]) << 24)
               | ((long) (0xff & buf[position]) << 32);
    }

    /**
     * Return next 40-bit unsigned int from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_uint5korr
     * 读取接下来5个字节,返回long
     */
    public final long getBeUlong40() {
        if (position + 4 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 4));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++]) << 32) | ((long) (0xff & buf[position++]) << 24)
               | ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 8)
               | ((long) (0xff & buf[position++]));
    }

    /**
     * Return 48-bit signed long from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint6korr
     * 在当前文件的位置 前进pos,读取接下来6个字节,返回long
     */
    public final long getLong48(final int pos) {
        final int position = origin + pos;

        if (pos + 5 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 5)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position])) | ((long) (0xff & buf[position + 1]) << 8)
               | ((long) (0xff & buf[position + 2]) << 16) | ((long) (0xff & buf[position + 3]) << 24)
               | ((long) (0xff & buf[position + 4]) << 32) | ((long) (buf[position + 5]) << 40);
    }

    /**
     * Return 48-bit signed long from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_sint6korr
     * 在当前文件的位置 前进pos,读取接下来6个字节,返回long
     */
    public final long getBeLong48(final int pos) {
        final int position = origin + pos;

        if (pos + 5 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 5)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position + 5])) | ((long) (0xff & buf[position + 4]) << 8)
               | ((long) (0xff & buf[position + 3]) << 16) | ((long) (0xff & buf[position + 2]) << 24)
               | ((long) (0xff & buf[position + 1]) << 32) | ((long) (buf[position]) << 40);
    }

    /**
     * Return next 48-bit signed long from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint6korr
     * 读取接下来6个字节,返回long
     */
    public final long getLong48() {
        if (position + 5 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 5));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++])) | ((long) (0xff & buf[position++]) << 8)
               | ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 24)
               | ((long) (0xff & buf[position++]) << 32) | ((long) (buf[position++]) << 40);
    }

    /**
     * Return next 48-bit signed long from buffer. (Big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_sint6korr
     * 读取接下来6个字节,返回long
     */
    public final long getBeLong48() {
        if (position + 5 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 5));

        byte[] buf = buffer;
        return ((long) (buf[position++]) << 40) | ((long) (0xff & buf[position++]) << 32)
               | ((long) (0xff & buf[position++]) << 24) | ((long) (0xff & buf[position++]) << 16)
               | ((long) (0xff & buf[position++]) << 8) | ((long) (0xff & buf[position++]));
    }

    /**
     * Return 48-bit unsigned long from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint6korr
     */
    public final long getUlong48(final int pos) {
        final int position = origin + pos;

        if (pos + 5 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 5)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position])) | ((long) (0xff & buf[position + 1]) << 8)
               | ((long) (0xff & buf[position + 2]) << 16) | ((long) (0xff & buf[position + 3]) << 24)
               | ((long) (0xff & buf[position + 4]) << 32) | ((long) (0xff & buf[position + 5]) << 40);
    }

    /**
     * Return 48-bit unsigned long from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_uint6korr
     */
    public final long getBeUlong48(final int pos) {
        final int position = origin + pos;

        if (pos + 5 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 5)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position + 5])) | ((long) (0xff & buf[position + 4]) << 8)
               | ((long) (0xff & buf[position + 3]) << 16) | ((long) (0xff & buf[position + 2]) << 24)
               | ((long) (0xff & buf[position + 1]) << 32) | ((long) (0xff & buf[position]) << 40);
    }

    /**
     * Return next 48-bit unsigned long from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint6korr
     */
    public final long getUlong48() {
        if (position + 5 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 5));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++])) | ((long) (0xff & buf[position++]) << 8)
               | ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 24)
               | ((long) (0xff & buf[position++]) << 32) | ((long) (0xff & buf[position++]) << 40);
    }

    /**
     * Return next 48-bit unsigned long from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_uint6korr
     */
    public final long getBeUlong48() {
        if (position + 5 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 5));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++]) << 40) | ((long) (0xff & buf[position++]) << 32)
               | ((long) (0xff & buf[position++]) << 24) | ((long) (0xff & buf[position++]) << 16)
               | ((long) (0xff & buf[position++]) << 8) | ((long) (0xff & buf[position++]));
    }

    /**
     * Return 56-bit unsigned int from buffer. (little-endian)
     * 在当前文件的位置 前进pos,读取接下来7个字节,返回long
     */
    public final long getUlong56(final int pos) {
        final int position = origin + pos;

        if (pos + 6 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 6)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position])) | ((long) (0xff & buf[position + 1]) << 8)
               | ((long) (0xff & buf[position + 2]) << 16) | ((long) (0xff & buf[position + 3]) << 24)
               | ((long) (0xff & buf[position + 4]) << 32) | ((long) (0xff & buf[position + 5]) << 40)
               | ((long) (0xff & buf[position + 6]) << 48);
    }

    /**
     * Return next 56-bit unsigned int from buffer. (little-endian)
     * 读取接下来7个字节,返回long
     */
    public final long getUlong56() {
        if (position + 6 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 6));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++])) | ((long) (0xff & buf[position++]) << 8)
               | ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 24)
               | ((long) (0xff & buf[position++]) << 32) | ((long) (0xff & buf[position++]) << 40)
               | ((long) (0xff & buf[position++]) << 48);
    }

    /**
     * Return 56-bit unsigned int from buffer. (big-endian)
     */
    public final long getBeUlong56(final int pos) {
        final int position = origin + pos;

        if (pos + 6 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 6)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position + 6])) | ((long) (0xff & buf[position + 5]) << 8)
               | ((long) (0xff & buf[position + 4]) << 16) | ((long) (0xff & buf[position + 3]) << 24)
               | ((long) (0xff & buf[position + 2]) << 32) | ((long) (0xff & buf[position + 1]) << 40)
               | ((long) (0xff & buf[position]) << 48);
    }

    /**
     * Return next 56-bit unsigned int from buffer. (big-endian)
     */
    public final long getBeUlong56() {
        if (position + 6 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 6));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++]) << 48) | ((long) (0xff & buf[position++]) << 40)
               | ((long) (0xff & buf[position++]) << 32) | ((long) (0xff & buf[position++]) << 24)
               | ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 8)
               | ((long) (0xff & buf[position++]));
    }

    /**
     * Return 64-bit signed long from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint8korr
     * 在当前文件的位置 前进pos,读取接下来8个字节,返回long
     */
    public final long getLong64(final int pos) {
        final int position = origin + pos;

        if (pos + 7 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 7)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position])) | ((long) (0xff & buf[position + 1]) << 8)
               | ((long) (0xff & buf[position + 2]) << 16) | ((long) (0xff & buf[position + 3]) << 24)
               | ((long) (0xff & buf[position + 4]) << 32) | ((long) (0xff & buf[position + 5]) << 40)
               | ((long) (0xff & buf[position + 6]) << 48) | ((long) (buf[position + 7]) << 56);
    }

    /**
     * Return 64-bit signed long from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_sint8korr
     * 在当前文件的位置 前进pos,读取接下来8个字节,返回long
     */
    public final long getBeLong64(final int pos) {
        final int position = origin + pos;

        if (pos + 7 >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                            + (pos < 0 ? pos : (pos + 7)));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position + 7])) | ((long) (0xff & buf[position + 6]) << 8)
               | ((long) (0xff & buf[position + 5]) << 16) | ((long) (0xff & buf[position + 4]) << 24)
               | ((long) (0xff & buf[position + 3]) << 32) | ((long) (0xff & buf[position + 2]) << 40)
               | ((long) (0xff & buf[position + 1]) << 48) | ((long) (buf[position]) << 56);
    }

    /**
     * Return next 64-bit signed long from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - sint8korr
     * 读取接下来8个字节,返回long
     */
    public final long getLong64() {
        if (position + 7 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 7));

        byte[] buf = buffer;
        return ((long) (0xff & buf[position++])) | ((long) (0xff & buf[position++]) << 8)
               | ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 24)
               | ((long) (0xff & buf[position++]) << 32) | ((long) (0xff & buf[position++]) << 40)
               | ((long) (0xff & buf[position++]) << 48) | ((long) (buf[position++]) << 56);
    }

    /**
     * Return next 64-bit signed long from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_sint8korr
     * 读取接下来8个字节,返回long
     */
    public final long getBeLong64() {
        if (position + 7 >= origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                               + (position - origin + 7));

        byte[] buf = buffer;
        return ((long) (buf[position++]) << 56) | ((long) (0xff & buf[position++]) << 48)
               | ((long) (0xff & buf[position++]) << 40) | ((long) (0xff & buf[position++]) << 32)
               | ((long) (0xff & buf[position++]) << 24) | ((long) (0xff & buf[position++]) << 16)
               | ((long) (0xff & buf[position++]) << 8) | ((long) (0xff & buf[position++]));
    }

    /* The max ulonglong - 0x ff ff ff ff ff ff ff ff */
    public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551615");

    /**
     * Return 64-bit unsigned long from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint8korr
     * 在当前文件的位置 前进pos,读取接下来8个字节,返回long
     */
    public final BigInteger getUlong64(final int pos) {
        final long long64 = getLong64(pos);

        return (long64 >= 0) ? BigInteger.valueOf(long64) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + long64));
    }

    /**
     * Return 64-bit unsigned long from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_uint8korr
     * 在当前文件的位置 前进pos,读取接下来8个字节,返回long
     */
    public final BigInteger getBeUlong64(final int pos) {
        final long long64 = getBeLong64(pos);

        return (long64 >= 0) ? BigInteger.valueOf(long64) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + long64));
    }

    /**
     * Return next 64-bit unsigned long from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - uint8korr
     * 读取接下来8个字节,返回long
     */
    public final BigInteger getUlong64() {
        final long long64 = getLong64();

        return (long64 >= 0) ? BigInteger.valueOf(long64) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + long64));
    }

    /**
     * Return next 64-bit unsigned long from buffer. (big-endian)
     * 
     * @see mysql-5.6.10/include/myisampack.h - mi_uint8korr
     * 读取接下来8个字节,返回long
     */
    public final BigInteger getBeUlong64() {
        final long long64 = getBeLong64();

        return (long64 >= 0) ? BigInteger.valueOf(long64) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + long64));
    }

    /**
     * Return 32-bit float from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - float4get
     */
    public final float getFloat32(final int pos) {
        return Float.intBitsToFloat(getInt32(pos));
    }

    /**
     * Return next 32-bit float from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - float4get
     */
    public final float getFloat32() {
        return Float.intBitsToFloat(getInt32());
    }

    /**
     * Return 64-bit double from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - float8get
     */
    public final double getDouble64(final int pos) {
        return Double.longBitsToDouble(getLong64(pos));
    }

    /**
     * Return next 64-bit double from buffer. (little-endian)
     * 
     * @see mysql-5.1.60/include/my_global.h - float8get
     */
    public final double getDouble64() {
        return Double.longBitsToDouble(getLong64());
    }

    public static final long NULL_LENGTH = ((long) ~0);

    /**
     * Return packed number from buffer. (little-endian) A Packed Integer has
     * the capacity of storing up to 8-byte integers, while small integers still
     * can use 1, 3, or 4 bytes. The value of the first byte determines how to
     * read the number, according to the following table.
     * <ul>
     * <li>0-250 The first byte is the number (in the range 0-250). No
     * additional bytes are used.</li>
     * <li>252 Two more bytes are used. The number is in the range 251-0xffff.</li>
     * <li>253 Three more bytes are used. The number is in the range
     * 0xffff-0xffffff.</li>
     * <li>254 Eight more bytes are used. The number is in the range
     * 0xffffff-0xffffffffffffffff.</li>
     * </ul>
     * That representation allows a first byte value of 251 to represent the SQL
     * NULL value.
     * 在当前文件的位置 前进pos,读取一个long返回
     */
    public final long getPackedLong(final int pos) {
        final int lead = getUint8(pos);
        if (lead < 251) return lead;

        switch (lead) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return getUint16(pos + 1);
            case 253:
                return getUint24(pos + 1);
            default: /* Must be 254 when here */
                return getUint32(pos + 1);
        }
    }

    /**
     * Return next packed number from buffer. (little-endian)
     * 
     * @see LogBuffer#getPackedLong(int)
     * 读取一个long返回
     */
    public final long getPackedLong() {
        final int lead = getUint8();
        if (lead < 251) return lead;

        switch (lead) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return getUint16();
            case 253:
                return getUint24();
            default: /* Must be 254 when here */
                final long value = getUint32();
                position += 4; /* ignore other */
                return value;
        }
    }

    /* default ANSI charset */
    public static final String ISO_8859_1 = "ISO-8859-1";

    /**
     * Return fix length string from buffer.
     */
    public final String getFixString(final int pos, final int len) {
        return getFixString(pos, len, ISO_8859_1);
    }

    /**
     * Return next fix length string from buffer.
     */
    public final String getFixString(final int len) {
        return getFixString(len, ISO_8859_1);
    }

    /**
     * Return fix length string from buffer.
     */
    public final String getFixString(final int pos, final int len, String charsetName) {
        if (pos + len > limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                             + (pos < 0 ? pos : (pos + len)));

        final int from = origin + pos;
        final int end = from + len;
        byte[] buf = buffer;
        int found = from;
        for (; (found < end) && buf[found] != '\0'; found++)
            /* empty loop */;

        try {
            return new String(buf, from, found - from, charsetName);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
        }
    }

    /**
     * Return next fix length string from buffer.
     */
    public final String getFixString(final int len, String charsetName) {
        if (position + len > origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                                + (position + len - origin));

        final int from = position;
        final int end = from + len;
        byte[] buf = buffer;
        int found = from;
        for (; (found < end) && buf[found] != '\0'; found++)
            /* empty loop */;

        try {
            String string = new String(buf, from, found - from, charsetName);
            position += len;
            return string;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
        }
    }

    /**
     * Return fix-length string from buffer without null-terminate checking. Fix
     * bug #17 {@link https://github.com/AlibabaTech/canal/issues/17 }
     */
    public final String getFullString(final int pos, final int len, String charsetName) {
        if (pos + len > limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                             + (pos < 0 ? pos : (pos + len)));

        try {
            return new String(buffer, origin + pos, len, charsetName);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
        }
    }

    /**
     * Return next fix-length string from buffer without null-terminate
     * checking. Fix bug #17 {@link https
     * ://github.com/AlibabaTech/canal/issues/17 }
     */
    public final String getFullString(final int len, String charsetName) {
        if (position + len > origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                                + (position + len - origin));

        try {
            String string = new String(buffer, position, len, charsetName);
            position += len;
            return string;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
        }
    }

    /**
     * Return dynamic length string from buffer.
     */
    public final String getString(final int pos) {
        return getString(pos, ISO_8859_1);
    }

    /**
     * Return next dynamic length string from buffer.
     */
    public final String getString() {
        return getString(ISO_8859_1);
    }

    /**
     * Return dynamic length string from buffer.
     */
    public final String getString(final int pos, String charsetName) {
        if (pos >= limit || pos < 0) throw new IllegalArgumentException("limit excceed: " + pos);

        byte[] buf = buffer;
        final int len = (0xff & buf[origin + pos]);
        if (pos + len + 1 > limit) throw new IllegalArgumentException("limit excceed: " + (pos + len + 1));

        try {
            return new String(buf, origin + pos + 1, len, charsetName);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
        }
    }

    /**
     * Return next dynamic length string from buffer.
     */
    public final String getString(String charsetName) {
        if (position >= origin + limit) throw new IllegalArgumentException("limit excceed: " + position);

        byte[] buf = buffer;
        final int len = (0xff & buf[position]);
        if (position + len + 1 > origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                                    + (position + len + 1 - origin));

        try {
            String string = new String(buf, position + 1, len, charsetName);
            position += len + 1;
            return string;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
        }
    }

    /**
     * Return 16-bit signed int from buffer. (big-endian)
     * 
     * @see mysql-5.1.60/include/myisampack.h - mi_sint2korr
     */
    private static final int getInt16BE(byte[] buffer, final int pos) {
        return ((buffer[pos]) << 8) | (0xff & buffer[pos + 1]);
    }

    /**
     * Return 24-bit signed int from buffer. (big-endian)
     * 
     * @see mysql-5.1.60/include/myisampack.h - mi_sint3korr
     */
    private static final int getInt24BE(byte[] buffer, final int pos) {
        return (buffer[pos] << 16) | ((0xff & buffer[pos + 1]) << 8) | (0xff & buffer[pos + 2]);
    }

    /**
     * Return 32-bit signed int from buffer. (big-endian)
     * 
     * @see mysql-5.1.60/include/myisampack.h - mi_sint4korr
     */
    private static final int getInt32BE(byte[] buffer, final int pos) {
        return (buffer[pos] << 24) | ((0xff & buffer[pos + 1]) << 16) | ((0xff & buffer[pos + 2]) << 8)
               | (0xff & buffer[pos + 3]);
    }

    /* decimal representation */
    public static final int DIG_PER_DEC1  = 9;
    public static final int DIG_BASE      = 1000000000;
    public static final int DIG_MAX       = DIG_BASE - 1;
    public static final int dig2bytes[]   = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
    public static final int powers10[]    = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

    public static final int DIG_PER_INT32 = 9;
    public static final int SIZE_OF_INT32 = 4;

    /**
     * Return big decimal from buffer.
     * 
     * @see mysql-5.1.60/strings/decimal.c - bin2decimal()
     */
    public final BigDecimal getDecimal(final int pos, final int precision, final int scale) {
        final int intg = precision - scale;
        final int frac = scale;
        final int intg0 = intg / DIG_PER_INT32;
        final int frac0 = frac / DIG_PER_INT32;
        final int intg0x = intg - intg0 * DIG_PER_INT32;
        final int frac0x = frac - frac0 * DIG_PER_INT32;

        final int binSize = intg0 * SIZE_OF_INT32 + dig2bytes[intg0x] + frac0 * SIZE_OF_INT32 + dig2bytes[frac0x];
        if (pos + binSize > limit || pos < 0) {
            throw new IllegalArgumentException("limit excceed: " + (pos < 0 ? pos : (pos + binSize)));
        }
        return getDecimal0(origin + pos, intg, frac, // NL
            intg0,
            frac0,
            intg0x,
            frac0x);
    }

    /**
     * Return next big decimal from buffer.
     * 
     * @see mysql-5.1.60/strings/decimal.c - bin2decimal()
     */
    public final BigDecimal getDecimal(final int precision, final int scale) {
        final int intg = precision - scale;
        final int frac = scale;
        final int intg0 = intg / DIG_PER_INT32;
        final int frac0 = frac / DIG_PER_INT32;
        final int intg0x = intg - intg0 * DIG_PER_INT32;
        final int frac0x = frac - frac0 * DIG_PER_INT32;

        final int binSize = intg0 * SIZE_OF_INT32 + dig2bytes[intg0x] + frac0 * SIZE_OF_INT32 + dig2bytes[frac0x];
        if (position + binSize > origin + limit) {
            throw new IllegalArgumentException("limit excceed: " + (position + binSize - origin));
        }

        BigDecimal decimal = getDecimal0(position, intg, frac, // NL
            intg0,
            frac0,
            intg0x,
            frac0x);
        position += binSize;
        return decimal;
    }

    /**
     * Return big decimal from buffer.
     * 
     * <pre>
     * Decimal representation in binlog seems to be as follows:
     * 
     * 1st bit - sign such that set == +, unset == -
     * every 4 bytes represent 9 digits in big-endian order, so that
     * if you print the values of these quads as big-endian integers one after
     * another, you get the whole number string representation in decimal. What
     * remains is to put a sign and a decimal dot.
     * 
     * 80 00 00 05 1b 38 b0 60 00 means:
     * 
     *   0x80 - positive 
     *   0x00000005 - 5
     *   0x1b38b060 - 456700000
     *   0x00       - 0
     * 
     * 54567000000 / 10^{10} = 5.4567
     * </pre>
     * 
     * @see mysql-5.1.60/strings/decimal.c - bin2decimal()
     * @see mysql-5.1.60/strings/decimal.c - decimal2string()
     */
    private final BigDecimal getDecimal0(final int begin, final int intg, final int frac, final int intg0,
                                         final int frac0, final int intg0x, final int frac0x) {
        final int mask = ((buffer[begin] & 0x80) == 0x80) ? 0 : -1;
        int from = begin;

        /* max string length */
        final int len = ((mask != 0) ? 1 : 0) + ((intg != 0) ? intg : 1) // NL
                        + ((frac != 0) ? 1 : 0) + frac;
        char[] buf = new char[len];
        int pos = 0;

        if (mask != 0) /* decimal sign */
        buf[pos++] = ('-');

        final byte[] d_copy = buffer;
        d_copy[begin] ^= 0x80; /* clear sign */
        int mark = pos;

        if (intg0x != 0) {
            final int i = dig2bytes[intg0x];
            int x = 0;
            switch (i) {
                case 1:
                    x = d_copy[from] /* one byte */;
                    break;
                case 2:
                    x = getInt16BE(d_copy, from);
                    break;
                case 3:
                    x = getInt24BE(d_copy, from);
                    break;
                case 4:
                    x = getInt32BE(d_copy, from);
                    break;
            }
            from += i;
            x ^= mask;
            if (x < 0 || x >= powers10[intg0x + 1]) {
                throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + powers10[intg0x + 1]);
            }
            if (x != 0 /* !digit || x != 0 */) {
                for (int j = intg0x; j > 0; j--) {
                    final int divisor = powers10[j - 1];
                    final int y = x / divisor;
                    if (mark < pos || y != 0) {
                        buf[pos++] = ((char) ('0' + y));
                    }
                    x -= y * divisor;
                }
            }
        }

        for (final int stop = from + intg0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32) {
            int x = getInt32BE(d_copy, from);
            x ^= mask;
            if (x < 0 || x > DIG_MAX) {
                throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
            }
            if (x != 0) {
                if (mark < pos) {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        final int divisor = powers10[i - 1];
                        final int y = x / divisor;
                        buf[pos++] = ((char) ('0' + y));
                        x -= y * divisor;
                    }
                } else {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        final int divisor = powers10[i - 1];
                        final int y = x / divisor;
                        if (mark < pos || y != 0) {
                            buf[pos++] = ((char) ('0' + y));
                        }
                        x -= y * divisor;
                    }
                }
            } else if (mark < pos) {
                for (int i = DIG_PER_DEC1; i > 0; i--)
                    buf[pos++] = ('0');
            }
        }

        if (mark == pos)
        /* fix 0.0 problem, only '.' may cause BigDecimal parsing exception. */
        buf[pos++] = ('0');

        if (frac > 0) {
            buf[pos++] = ('.');
            mark = pos;

            for (final int stop = from + frac0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32) {
                int x = getInt32BE(d_copy, from);
                x ^= mask;
                if (x < 0 || x > DIG_MAX) {
                    throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
                }
                if (x != 0) {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        final int divisor = powers10[i - 1];
                        final int y = x / divisor;
                        buf[pos++] = ((char) ('0' + y));
                        x -= y * divisor;
                    }
                } else {
                    for (int i = DIG_PER_DEC1; i > 0; i--)
                        buf[pos++] = ('0');
                }
            }

            if (frac0x != 0) {
                final int i = dig2bytes[frac0x];
                int x = 0;
                switch (i) {
                    case 1:
                        x = d_copy[from] /* one byte */;
                        break;
                    case 2:
                        x = getInt16BE(d_copy, from);
                        break;
                    case 3:
                        x = getInt24BE(d_copy, from);
                        break;
                    case 4:
                        x = getInt32BE(d_copy, from);
                        break;
                }
                x ^= mask;
                if (x != 0) {
                    final int dig = DIG_PER_DEC1 - frac0x;
                    x *= powers10[dig];
                    if (x < 0 || x > DIG_MAX) {
                        throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
                    }
                    for (int j = DIG_PER_DEC1; j > dig; j--) {
                        final int divisor = powers10[j - 1];
                        final int y = x / divisor;
                        buf[pos++] = ((char) ('0' + y));
                        x -= y * divisor;
                    }
                }
            }

            if (mark == pos)
            /* make number more friendly */
            buf[pos++] = ('0');
        }

        d_copy[begin] ^= 0x80; /* restore sign */
        String decimal = String.valueOf(buf, 0, pos);
        return new BigDecimal(decimal);
    }

    /**
     * Fill MY_BITMAP structure from buffer.
     * 
     * @param len The length of MY_BITMAP in bits.
     */
    public final void fillBitmap(BitSet bitmap, final int pos, final int len) {
        if (pos + ((len + 7) / 8) > limit || pos < 0) throw new IllegalArgumentException("limit excceed: "
                                                                                         + (pos + (len + 7) / 8));

        fillBitmap0(bitmap, origin + pos, len);
    }

    /**
     * Fill next MY_BITMAP structure from buffer.
     * 
     * @param len The length of MY_BITMAP in bits.
     */
    public final void fillBitmap(BitSet bitmap, final int len) {
        if (position + ((len + 7) / 8) > origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                                            + (position
                                                                                               + ((len + 7) / 8) - origin));

        position = fillBitmap0(bitmap, position, len);
    }

    /**
     * Fill MY_BITMAP structure from buffer.
     * 
     * @param len The length of MY_BITMAP in bits.
     */
    private final int fillBitmap0(BitSet bitmap, int pos, final int len) {
        final byte[] buf = buffer;

        for (int bit = 0; bit < len; bit += 8) {
            int flag = ((int) buf[pos++]) & 0xff;
            if (flag == 0) continue;
            if ((flag & 0x01) != 0) bitmap.set(bit);
            if ((flag & 0x02) != 0) bitmap.set(bit + 1);
            if ((flag & 0x04) != 0) bitmap.set(bit + 2);
            if ((flag & 0x08) != 0) bitmap.set(bit + 3);
            if ((flag & 0x10) != 0) bitmap.set(bit + 4);
            if ((flag & 0x20) != 0) bitmap.set(bit + 5);
            if ((flag & 0x40) != 0) bitmap.set(bit + 6);
            if ((flag & 0x80) != 0) bitmap.set(bit + 7);
        }
        return pos;
    }

    /**
     * Return MY_BITMAP structure from buffer.
     * 
     * @param len The length of MY_BITMAP in bits.
     */
    public final BitSet getBitmap(final int pos, final int len) {
        BitSet bitmap = new BitSet(len);
        fillBitmap(bitmap, pos, len);
        return bitmap;
    }

    /**
     * Return next MY_BITMAP structure from buffer.
     * 
     * @param len The length of MY_BITMAP in bits.
     */
    public final BitSet getBitmap(final int len) {
        BitSet bitmap = new BitSet(len);
        fillBitmap(bitmap, len);
        return bitmap;
    }

    /**
     * Fill n bytes into output stream.
     * 将buffer中的内容写入到out中,从buffer的origin + pos开始读取数据,读取len个字节
     */
    public final void fillOutput(OutputStream out, final int pos, final int len) throws IOException {
        if (pos + len > limit || pos < 0) throw new IllegalArgumentException("limit excceed: " + (pos + len));

        out.write(buffer, origin + pos, len);
    }

    /**
     * Fill next n bytes into output stream
     * 将buffer中的内容写入到out中,从buffer的position开始读取数据,读取len个字节
     */
    public final void fillOutput(OutputStream out, final int len) throws IOException {
        if (position + len > origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                                + (position + len - origin));

        out.write(buffer, position, len);
        position += len;
    }

    /**
     * Fill n bytes in this buffer.
     * 将buffer的origin + pos位置开始复制字节,复制len个字节,复制到dest字节数组中,从dest的destPos位置开始覆盖
     */
    public final void fillBytes(final int pos, byte[] dest, final int destPos, final int len) {
        if (pos + len > limit || pos < 0) throw new IllegalArgumentException("limit excceed: " + (pos + len));

        System.arraycopy(buffer, origin + pos, dest, destPos, len);
    }

    /**
     * Fill next n bytes in this buffer.
     * 将buffer的position位置开始复制字节,复制len个字节,复制到dest字节数组中,从dest的destPos位置开始覆盖
     * 并且position位置会跟随变化
     */
    public final void fillBytes(byte[] dest, final int destPos, final int len) {
        if (position + len > origin + limit) throw new IllegalArgumentException("limit excceed: "
                                                                                + (position + len - origin));

        System.arraycopy(buffer, position, dest, destPos, len);
        position += len;
    }

    /**
     * Return n-byte data from buffer.
     * 将buffer的origin + pos位置开始复制字节,复制len个字节,复制到buf字节数组中
     */
    public final byte[] getData(final int pos, final int len) {
        byte[] buf = new byte[len];
        fillBytes(pos, buf, 0, len);
        return buf;
    }

    /**
     * Return next n-byte data from buffer.
     * 将buffer的position位置开始复制字节,复制len个字节,复制到buf字节数组中
     * 并且position位置会跟随变化
     */
    public final byte[] getData(final int len) {
        byte[] buf = new byte[len];
        fillBytes(buf, 0, len);
        return buf;
    }

    /**
     * Return all remaining data from buffer.
     * 将buffer的position位置开始复制剩余全部字节,返回全部字节的字节数组
     * 并且position位置会跟随变化
     */
    public final byte[] getData() {
        return getData(0, limit);
    }

    /**
     * Return full hexdump from position.
     */
    public final String hexdump(final int pos) {
        if ((limit - pos) > 0) {
            final int begin = origin + pos;
            final int end = origin + limit;

            byte[] buf = buffer;
            StringBuilder dump = new StringBuilder();
            dump.append(Integer.toHexString(buf[begin] >> 4));
            dump.append(Integer.toHexString(buf[begin] & 0xf));
            for (int i = begin + 1; i < end; i++) {
                dump.append("_");
                dump.append(Integer.toHexString(buf[i] >> 4));
                dump.append(Integer.toHexString(buf[i] & 0xf));
            }

            return dump.toString();
        }
        return "";
    }

    /**
     * Return hexdump from position, for len bytes.
     */
    public final String hexdump(final int pos, final int len) {
        if ((limit - pos) > 0) {
            final int begin = origin + pos;
            final int end = Math.min(begin + len, origin + limit);

            byte[] buf = buffer;
            StringBuilder dump = new StringBuilder();
            dump.append(Integer.toHexString(buf[begin] >> 4));
            dump.append(Integer.toHexString(buf[begin] & 0xf));
            for (int i = begin + 1; i < end; i++) {
                dump.append("_");
                dump.append(Integer.toHexString(buf[i] >> 4));
                dump.append(Integer.toHexString(buf[i] & 0xf));
            }

            return dump.toString();
        }
        return "";
    }
}
