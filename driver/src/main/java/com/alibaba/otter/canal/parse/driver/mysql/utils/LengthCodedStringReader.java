package com.alibaba.otter.canal.parse.driver.mysql.utils;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;

//在data中读取body长度和body内容,返回body的内容
public class LengthCodedStringReader {

    public static final String CODE_PAGE_1252 = "UTF-8";//body字节的默认编码

    private String             encoding;//body字节的编码
    private int                index          = 0;      // 数组下标

    public LengthCodedStringReader(String encoding, int startIndex){
        this.encoding = encoding;
        this.index = startIndex;
    }

    //在data中读取body长度和body内容,返回body的内容
    public String readLengthCodedString(byte[] data) throws IOException {
        byte[] lengthBytes = ByteHelper.readBinaryCodedLengthBytes(data, getIndex());//读取body长度所占用的字节
        long length = ByteHelper.readLengthCodedBinary(data, getIndex());//获取长度
        setIndex(getIndex() + lengthBytes.length);//更改index位置
        if (ByteHelper.NULL_LENGTH == length) {//表示读取到结尾了,已经没有字节可以读取了
            return null;
        }

        try {
        	//读取length长度字节,转换成字符串
            return new String(ArrayUtils.subarray(data, getIndex(), (int) (getIndex() + length)),
                encoding == null ? CODE_PAGE_1252 : encoding);
        } finally {//移动index位置
            setIndex((int) (getIndex() + length));
        }

    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}
