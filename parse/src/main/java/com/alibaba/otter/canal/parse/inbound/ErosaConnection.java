package com.alibaba.otter.canal.parse.inbound;

import java.io.IOException;

/**
 * 通用的Erosa的链接接口, 用于一般化处理mysql/oracle的解析过程
 * 
 * @author: yuanzu Date: 12-9-20 Time: 下午2:47
 */
public interface ErosaConnection {

    //创建连接
    public void connect() throws IOException;

    //重新连接
    public void reconnect() throws IOException;

    //销毁连接
    public void disconnect() throws IOException;

    //是否连接
    public boolean isConnected();

    /**
     * 用于快速数据查找,和dump的区别在于，seek会只给出部分的数据
     */
    public void seek(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException;

    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException;

    public void dump(long timestamp, SinkFunction func) throws IOException;

    //重新产生一个子进程
    ErosaConnection fork();
}
