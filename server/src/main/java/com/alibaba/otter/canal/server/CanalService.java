package com.alibaba.otter.canal.server;

import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;

/**
 * 一个canal server提供的服务
 */
public interface CanalService {

    //注册一个客户端
    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;

    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;

    //一个客户端到服务区来获取事件信息,但是提取后要提交给服务器
    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;

    //客户端可以到服务区来获取事件信息,不用提交
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
                                                                                                    throws CanalServerException;
    //客户端提交一个批处理
    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;

    //客户端回滚一个批处理
    void rollback(ClientIdentity clientIdentity) throws CanalServerException;

    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
