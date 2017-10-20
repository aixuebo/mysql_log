package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;

/**
 * quit cmd
 * 
 * @author agapple 2016年3月1日 下午8:33:02
 * @since 1.0.22
 * 发送退出命令
 */
public class QuitCommandPacket extends CommandPacket {

    public static final byte[] QUIT = new byte[] { 1, 0, 0, 0, 1 };

    public QuitCommandPacket(){
        setCommand((byte) 0x01);
    }

    //因为不需要收到信息.因此不需要该方法的实现
    @Override
    public void fromBytes(byte[] data) throws IOException {

    }

    //发出去的命令字节数组
    @Override
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(getCommand());
        out.write(QUIT);
        return out.toByteArray();
    }

}
