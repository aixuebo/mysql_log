package com.alibaba.otter.canal.parse.driver.mysql.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class ByteHelper {

    public static final long NULL_LENGTH = -1;//表示读取到结尾了,已经没有字节可以读取了

    //从data的index开始读取数据,一直读取到终止字符为止,返回读取的字节数组
    public static byte[] readNullTerminatedBytes(byte[] data, int index) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = index; i < data.length; i++) {
            byte item = data[i];
            if (item == MSC.NULL_TERMINATED_STRING_DELIMITER) {
                break;
            }
            out.write(item);
        }
        return out.toByteArray();
    }

    //将str的字节写入到out中,并且追加null的终止字符
    public static void writeNullTerminatedString(String str, ByteArrayOutputStream out) throws IOException {
        out.write(str.getBytes());
        out.write(MSC.NULL_TERMINATED_STRING_DELIMITER);
    }

    //将data的字节写入到out中,并且追加null的终止字符
    public static void writeNullTerminated(byte[] data, ByteArrayOutputStream out) throws IOException {
        out.write(data);
        out.write(MSC.NULL_TERMINATED_STRING_DELIMITER);
    }

    //读取固定长度字符
    public static byte[] readFixedLengthBytes(byte[] data, int index, int length) {
        byte[] bytes = new byte[length];
        System.arraycopy(data, index, bytes, 0, length);
        return bytes;
    }

    /**
     * Read 4 bytes in Little-endian byte order.
     * 
     * @param data, the original byte array
     * @param index, start to read from.
     * @return
     * 读取4个字节的int,返回long
     */
    public static long readUnsignedIntLittleEndian(byte[] data, int index) {
        long result = (long) (data[index] & 0xFF) | (long) ((data[index + 1] & 0xFF) << 8)
                      | (long) ((data[index + 2] & 0xFF) << 16) | (long) ((data[index + 3] & 0xFF) << 24);
        return result;
    }

    //读取8个字节的long
    public static long readUnsignedLongLittleEndian(byte[] data, int index) {
        long accumulation = 0;
        int position = index;
        for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
            accumulation |= (long) ((data[position++] & 0xff) << shiftBy);
        }
        return accumulation;
    }

    //读取2个字节的short,返回int
    public static int readUnsignedShortLittleEndian(byte[] data, int index) {
        int result = (data[index] & 0xFF) | ((data[index + 1] & 0xFF) << 8);
        return result;
    }

    //读取3个字节,返回int
    public static int readUnsignedMediumLittleEndian(byte[] data, int index) {
        int result = (data[index] & 0xFF) | ((data[index + 1] & 0xFF) << 8) | ((data[index + 2] & 0xFF) << 16);
        return result;
    }

    //根据data的index位置是什么类型,做相应的long值转换
    public static long readLengthCodedBinary(byte[] data, int index) throws IOException {
        int firstByte = data[index] & 0xFF;//其实返回的是byte,转换成int.说明该int使用一个byte就可以表达了
        switch (firstByte) {
            case 251:
                return NULL_LENGTH;//直接返回-1
            case 252:
                return readUnsignedShortLittleEndian(data, index + 1);//2个字节表示的int
            case 253:
                return readUnsignedMediumLittleEndian(data, index + 1);//3个字节表示的int
            case 254:
                return readUnsignedLongLittleEndian(data, index + 1);//8个字节的long
            default:
                return firstByte;//就是正常的int,不需要转换
        }
    }

    //根据data的index位置不同类型,返回int的字节数组
    public static byte[] readBinaryCodedLengthBytes(byte[] data, int index) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(data[index]);//写入第一个字节,其实返回的是byte,转换成int.说明该int使用一个byte就可以表达了

        byte[] buffer = null;
        int value = data[index] & 0xFF;//类型
        if (value == 251) {
            buffer = new byte[0];
        }
        if (value == 252) {//2个字节表示int
            buffer = new byte[2];
        }
        if (value == 253) {//3个字节表示int
            buffer = new byte[3];
        }
        if (value == 254) {//8个字节表示long
            buffer = new byte[8];
        }
        if (buffer != null) {//读取buffer长度的信息
            System.arraycopy(data, index + 1, buffer, 0, buffer.length);
            out.write(buffer);
        }

        //返回读取的字节数组
        return out.toByteArray();
    }

    //将long写成4个字节,写入到输出中
    public static void writeUnsignedIntLittleEndian(long data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) (data >>> 8));
        out.write((byte) (data >>> 16));
        out.write((byte) (data >>> 24));
    }

    //将int写成2个字节,写入到输出中
    public static void writeUnsignedShortLittleEndian(int data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) ((data >>> 8) & 0xFF));
    }

    //将int写成3个字节,写入到输出中
    public static void writeUnsignedMediumLittleEndian(int data, ByteArrayOutputStream out) {
        out.write((byte) (data & 0xFF));
        out.write((byte) ((data >>> 8) & 0xFF));
        out.write((byte) ((data >>> 16) & 0xFF));
    }

    //将data数据写入到输出中
    public static void writeBinaryCodedLengthBytes(byte[] data, ByteArrayOutputStream out) throws IOException {
        // 1. write length byte/bytes 写入字节长度
        if (data.length < 252) {//说明该int使用一个byte就可以表达了
            out.write((byte) data.length);
        } else if (data.length < (1 << 16L)) {//65536,超过了2个字节
            out.write((byte) 252);
            writeUnsignedShortLittleEndian(data.length, out);//用2个字节表示int
        } else if (data.length < (1 << 24L)) {//用3个字节表示int
            out.write((byte) 253);
            writeUnsignedMediumLittleEndian(data.length, out);
        } else {
            out.write((byte) 254);//用8个字节表示int
            writeUnsignedIntLittleEndian(data.length, out);
        }
        // 2. write real data followed length byte/bytes 真正写入data数据
        out.write(data);
    }

    //将data的数据固定长度写入到输出中
    public static void writeFixedLengthBytes(byte[] data, int index, int length, ByteArrayOutputStream out) {
        for (int i = index; i < index + length; i++) {
            out.write(data[i]);
        }
    }

    //将data的数据固定长度写入到输出中
    public static void writeFixedLengthBytesFromStart(byte[] data, int length, ByteArrayOutputStream out) {
        writeFixedLengthBytes(data, 0, length, out);
    }

}
