package com.alibaba.otter.canal.server.netty.handler;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;

/**
 * 解析对应的header信息
 * 
 * @author jianghang 2012-10-24 上午11:31:39
 * @version 1.0.0
 * 读取header头内容,通过头内容,获取整个包内容
 */
public class FixedHeaderFrameDecoder extends ReplayingDecoder<VoidEnum> {

    //将内容存储到buffer参数中
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, VoidEnum state)
                                                                                                             throws Exception {
        return buffer.readBytes(buffer.readInt());//先读取一个size,然后在读取size对应的字节大小,此时表示一个完成的包被读取完成
    }
}
