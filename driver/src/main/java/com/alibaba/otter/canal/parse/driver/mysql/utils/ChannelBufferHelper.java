package com.alibaba.otter.canal.parse.driver.mysql.utils;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.IPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;

public class ChannelBufferHelper {

    protected transient final Logger logger = LoggerFactory.getLogger(ChannelBufferHelper.class);

    //从buffer中读取4个字节,组成HeaderPacket对象
    public final HeaderPacket assembleHeaderPacket(ChannelBuffer buffer) {
        HeaderPacket header = new HeaderPacket();
        byte[] headerBytes = new byte[MSC.HEADER_PACKET_LENGTH];//包头的长度.4个字节
        buffer.readBytes(headerBytes);
        header.fromBytes(headerBytes);//用4个字节组成HeaderPacket对象
        return header;
    }

    //根据头文件内容,从buffer中读取body
    public final PacketWithHeaderPacket assembleBodyPacketWithHeader(ChannelBuffer buffer, HeaderPacket header,
                                                                     PacketWithHeaderPacket body) throws IOException {
        if (body.getHeader() == null) {
            body.setHeader(header);
        }
        logger.debug("body packet type:{}", body.getClass());
        logger.debug("read body packet with packet length: {} ", header.getPacketBodyLength());
        byte[] packetBytes = new byte[header.getPacketBodyLength()];//设置body字节长度

        logger.debug("readable bytes before reading body:{}", buffer.readableBytes());
        buffer.readBytes(packetBytes);//读取body内容
        body.fromBytes(packetBytes);//将body字节数组存储到对象中

        logger.debug("body packet: {}", body);
        return body;
    }

    //header包的内容组装成ChannelBuffer对象,序号累加1
    public final ChannelBuffer createHeaderWithPacketNumberPlusOne(int bodyLength, byte packetNumber) {
        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(bodyLength);
        header.setPacketSequenceNumber((byte) (packetNumber + 1));
        return ChannelBuffers.wrappedBuffer(header.toBytes());
    }

    //header包的内容组装成ChannelBuffer对象
    public final ChannelBuffer createHeader(int bodyLength, byte packetNumber) {
        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(bodyLength);
        header.setPacketSequenceNumber(packetNumber);
        return ChannelBuffers.wrappedBuffer(header.toBytes());
    }

    //发送头对象和body内容
    public final ChannelBuffer buildChannelBufferFromCommandPacket(IPacket packet) throws IOException {
        byte[] bodyBytes = packet.toBytes();//将要发送的信息组装成字节数组
        ChannelBuffer header = createHeader(bodyBytes.length, (byte) 0);//创建head头对象的字节数组
        return ChannelBuffers.wrappedBuffer(header, ChannelBuffers.wrappedBuffer(bodyBytes));//发送头对象和body内容
    }
}
