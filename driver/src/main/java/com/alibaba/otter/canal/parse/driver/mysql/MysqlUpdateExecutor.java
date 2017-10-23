package com.alibaba.otter.canal.parse.driver.mysql;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.packets.client.QueryCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.OKPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;

/**
 * 默认输出的数据编码为UTF-8，如有需要请正确转码
 * 
 * @author jianghang 2013-9-4 上午11:51:11
 * @since 1.0.0
 * 执行mysql的命令,比如set wait_timeout=9999999
 */
public class MysqlUpdateExecutor {

    private static final Logger logger = LoggerFactory.getLogger(MysqlUpdateExecutor.class);

    private SocketChannel       channel;

    public MysqlUpdateExecutor(MysqlConnector connector){
        if (!connector.isConnected()) {
            throw new RuntimeException("should execute connector.connect() first");
        }

        this.channel = connector.getChannel();
    }

    public MysqlUpdateExecutor(SocketChannel ch){
        this.channel = ch;
    }

    //例如updateString为set wait_timeout=9999999
    public OKPacket update(String updateString) throws IOException {
        QueryCommandPacket cmd = new QueryCommandPacket();//发送命令sql
        cmd.setQueryString(updateString);
        byte[] bodyBytes = cmd.toBytes();
        PacketManager.write(channel, bodyBytes);

        logger.debug("read update result...");
        byte[] body = PacketManager.readBytes(channel, PacketManager.readHeader(channel, 4).getPacketBodyLength());//等待mysql的response,获取response的字节长度 以及 内容
        if (body[0] < 0) {
            ErrorPacket packet = new ErrorPacket();
            packet.fromBytes(body);
            throw new IOException(packet + "\n with command: " + updateString);
        }

        OKPacket packet = new OKPacket();//表示response返回的内容成功
        packet.fromBytes(body);
        return packet;
    }
}
