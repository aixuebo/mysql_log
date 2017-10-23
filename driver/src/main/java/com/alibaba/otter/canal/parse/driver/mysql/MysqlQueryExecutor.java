package com.alibaba.otter.canal.parse.driver.mysql;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.QueryCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetHeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.RowDataPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;

/**
 * 默认输出的数据编码为UTF-8，如有需要请正确转码
 * 
 * @author jianghang 2013-9-4 上午11:50:26
 * @since 1.0.0
 * 发送查询的sql
 */
public class MysqlQueryExecutor {

    private SocketChannel channel;

    public MysqlQueryExecutor(MysqlConnector connector){
        if (!connector.isConnected()) {
            throw new RuntimeException("should execute connector.connect() first");
        }

        this.channel = connector.getChannel();
    }

    public MysqlQueryExecutor(SocketChannel ch){
        this.channel = ch;
    }

    /**
     * (Result Set Header Packet) the number of columns <br>
     * (Field Packets) column descriptors <br>
     * (EOF Packet) marker: end of Field Packets <br>
     * (Row Data Packets) row contents <br>
     * (EOF Packet) marker: end of Data Packets
     * 
     * @param queryString
     * @return
     * @throws IOException
     */
    public ResultSetPacket query(String queryString) throws IOException {
        QueryCommandPacket cmd = new QueryCommandPacket();
        cmd.setQueryString(queryString);
        byte[] bodyBytes = cmd.toBytes();
        PacketManager.write(channel, bodyBytes);//发送查询的sql
        byte[] body = readNextPacket();//读取下一个包--response返回的包

        if (body[0] < 0) {
            ErrorPacket packet = new ErrorPacket();
            packet.fromBytes(body);
            throw new IOException(packet + "\n with command: " + queryString);
        }

        ResultSetHeaderPacket rsHeader = new ResultSetHeaderPacket();//返回结果集
        rsHeader.fromBytes(body);

        //返回的所有列信息----mysql返回的查询的结果集中列的schema信息---该对象表示一个列的schema
        List<FieldPacket> fields = new ArrayList<FieldPacket>();
        for (int i = 0; i < rsHeader.getColumnCount(); i++) {//循环每一个列
            FieldPacket fp = new FieldPacket();
            fp.fromBytes(readNextPacket());//继续读取每一个列的信息
            fields.add(fp);
        }

        readEofPacket();//说明是最后了

        //处理每一行数据
        List<RowDataPacket> rowData = new ArrayList<RowDataPacket>();
        while (true) {
            body = readNextPacket();
            if (body[0] == -2) {//说明行读取完
                break;
            }
            RowDataPacket rowDataPacket = new RowDataPacket();
            rowDataPacket.fromBytes(body);
            rowData.add(rowDataPacket);
        }

        //组成返回结果集
        ResultSetPacket resultSet = new ResultSetPacket();
        resultSet.getFieldDescriptors().addAll(fields);
        for (RowDataPacket r : rowData) {
            resultSet.getFieldValues().addAll(r.getColumns());//设置每一行中列的信息集合
        }
        resultSet.setSourceAddress(channel.socket().getRemoteSocketAddress());//设置服务器的ip组成的socket信息

        return resultSet;
    }

    //读取结束包
    private void readEofPacket() throws IOException {
        byte[] eofBody = readNextPacket();
        if (eofBody[0] != -2) {//说明不是结束包,则抛异常
            throw new IOException("EOF Packet is expected, but packet with field_count=" + eofBody[0] + " is found.");
        }
    }

    //读取下一个包
    protected byte[] readNextPacket() throws IOException {
        HeaderPacket h = PacketManager.readHeader(channel, 4);//要读取包的长度
        return PacketManager.readBytes(channel, h.getPacketBodyLength());//包的内容
    }
}
