package com.alibaba.otter.canal.client;

import java.net.SocketAddress;

/**
 * 集群节点访问控制接口
 * 也就是客户端访问一个集群,让集群为客户端提供事件内容去消费
 * @author jianghang 2012-10-29 下午07:55:41
 * @version 1.0.0
 */
public interface CanalNodeAccessStrategy {

    SocketAddress currentNode();

    SocketAddress nextNode();
}
