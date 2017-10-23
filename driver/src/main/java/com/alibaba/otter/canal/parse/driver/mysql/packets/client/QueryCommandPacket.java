package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;

//客户端发送查询命令 例如set wait_timeout=9999999
public class QueryCommandPacket extends CommandPacket {

    private String queryString;

    public QueryCommandPacket(){
        setCommand((byte) 0x03);
    }

    //接到字节信息后,进行处理
    public void fromBytes(byte[] data) throws IOException {
    }

    //将发送的信息转换成字节数组
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(getCommand());
        out.write(getQueryString().getBytes("UTF-8"));// 链接建立时默认指定编码为UTF-8
        return out.toByteArray();
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public String getQueryString() {
        return queryString;
    }

}
