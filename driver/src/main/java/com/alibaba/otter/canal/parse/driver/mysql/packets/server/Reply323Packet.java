package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

public class Reply323Packet extends PacketWithHeaderPacket {

    public byte[] seed;//要发送的数据包

    public void fromBytes(byte[] data) throws IOException {

    }

    public byte[] toBytes() throws IOException {
        if (seed == null) {
            return new byte[] { (byte) 0 };
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteHelper.writeNullTerminated(seed, out);//将data的字节写入到out中,并且追加null的终止字符
            return out.toByteArray();
        }
    }

}
