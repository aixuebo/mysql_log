package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.LengthCodedStringReader;

//表示mysql返回的查询的结果集中一行数据的详细信息
public class RowDataPacket extends PacketWithHeaderPacket {

    private List<String> columns = new ArrayList<String>();

    public void fromBytes(byte[] data) throws IOException {
        int index = 0;
        LengthCodedStringReader reader = new LengthCodedStringReader(null, index);
        do {
            getColumns().add(reader.readLengthCodedString(data));
        } while (reader.getIndex() < data.length);
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String toString() {
        return "RowDataPacket [columns=" + columns + "]";
    }

}
