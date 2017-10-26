package com.alibaba.otter.canal.client.impl;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.client.CanalNodeAccessStrategy;

/**
 * 简单版本的node访问实现
 * 
 * @author jianghang 2012-10-29 下午08:00:23
 * @version 1.0.0
 */
public class SimpleNodeAccessStrategy implements CanalNodeAccessStrategy {

    private List<SocketAddress> nodes = new ArrayList<SocketAddress>();//持有一组Ip集合,该集群的ip集合会切换为客户端进行服务
    private int                 index = 0;//当前使用的ip

    public SimpleNodeAccessStrategy(List<? extends SocketAddress> nodes){
        if (nodes == null || nodes.size() < 1) {
            throw new IllegalArgumentException("at least 1 node required.");
        }
        this.nodes.addAll(nodes);
    }

    public SocketAddress nextNode() {
        try {
            return nodes.get(index);
        } finally {
            index = (index + 1) % nodes.size();
        }
    }

    @Override
    public SocketAddress currentNode() {
        return nodes.get(index);
    }

}
