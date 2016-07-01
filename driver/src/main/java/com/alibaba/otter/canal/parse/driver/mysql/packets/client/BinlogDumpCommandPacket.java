package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

/**
 * COM_BINLOG_DUMP
 * 
 * @author fujohnwang
 * @since 1.0
 * 发送binlog命令的包
 */
public class BinlogDumpCommandPacket extends CommandPacket {

    /** BINLOG_DUMP options */
    public static final int BINLOG_DUMP_NON_BLOCK           = 1;
    public static final int BINLOG_SEND_ANNOTATE_ROWS_EVENT = 2;//binlog的注释
    public long             binlogPosition;//binlog的开始位置
    public long             slaveServerId;//slave节点的id
    public String           binlogFileName;//日志名字

    public BinlogDumpCommandPacket(){
        setCommand((byte) 0x12);//命令是12
    }

    //不需要解析
    public void fromBytes(byte[] data) {
        // bypass
    }

    /**
     * <pre>
     * Bytes                        Name
     *  -----                        ----
     *  1                            command
     *  n                            arg
     *  1个命令和n个参数
     *  --------------------------------------------------------
     *  Bytes                        Name
     *  -----                        ----
     *  4                            binlog position to start at (little endian),4个字节,表示要binlog的开始位置
     *  2                            binlog flags (currently not used; always 0) 当前总是0
     *  4                            server_id of the slave (little endian) 4个字节表示哪个slave节点的标识
     *  n                            binlog file name (optional) n个字节的binlog文件名字
     * 
     * </pre>
     * 生成body的内容字节数组
     * 发送要binlog的请求
     */
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 0. write command number 输入命令编码
        out.write(getCommand());
        // 1. write 4 bytes bin-log position to start at使用4个字节,发送要binlog的开始位置
        ByteHelper.writeUnsignedIntLittleEndian(binlogPosition, out);
        // 2. write 2 bytes bin-log flags
        int binlog_flags = 0;
        binlog_flags |= BINLOG_SEND_ANNOTATE_ROWS_EVENT;//0 | 2 = 0 
        out.write(binlog_flags);
        out.write(0x00);
        // 3. write 4 bytes server id of the slave 写入slave节点ID标识符
        ByteHelper.writeUnsignedIntLittleEndian(this.slaveServerId, out);
        // 4. write bin-log file name if necessary //如果需要binlog文件名,则写入
        if (StringUtils.isNotEmpty(this.binlogFileName)) {
            out.write(this.binlogFileName.getBytes());
        }
        return out.toByteArray();
    }

}
