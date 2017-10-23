package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

//查询的结果集
public class ResultSetPacket {

    private SocketAddress     sourceAddress;//哪个socket服务器返回给我的结果
    private List<FieldPacket> fieldDescriptors = new ArrayList<FieldPacket>();//结果集的schema信息
    private List<String>      fieldValues      = new ArrayList<String>();//结果集的行信息--是fieldDescriptors.size的若干倍

    public void setFieldDescriptors(List<FieldPacket> fieldDescriptors) {
        this.fieldDescriptors = fieldDescriptors;
    }

    public List<FieldPacket> getFieldDescriptors() {
        return fieldDescriptors;
    }

    public void setFieldValues(List<String> fieldValues) {
        this.fieldValues = fieldValues;
    }

    public List<String> getFieldValues() {
        return fieldValues;
    }

    public void setSourceAddress(SocketAddress sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public SocketAddress getSourceAddress() {
        return sourceAddress;
    }

    public String toString() {
        return "ResultSetPacket [fieldDescriptors=" + fieldDescriptors + ", fieldValues=" + fieldValues
               + ", sourceAddress=" + sourceAddress + "]";
    }

}
