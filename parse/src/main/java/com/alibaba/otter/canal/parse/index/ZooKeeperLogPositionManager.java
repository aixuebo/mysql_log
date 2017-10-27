package com.alibaba.otter.canal.parse.index;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * 基于zk的实现---zookeeper上记录每一个队列已经同步master的哪个日志位置了
 * 
 * @author jianghang 2012-7-7 上午10:08:27
 * @version 1.0.0
 */
public class ZooKeeperLogPositionManager extends AbstractCanalLifeCycle implements CanalLogPositionManager {

    private ZkClientx zkClientx;

    public void start() {
        super.start();
        Assert.notNull(zkClientx);
    }

    public void stop() {
        super.stop();
    }

    //获取该目的地的LogPosition信息
    public LogPosition getLatestIndexBy(String destination) {
        String path = ZookeeperPathUtils.getParsePath(destination);
        byte[] data = zkClientx.readData(path, true);
        if (data == null || data.length == 0) {
            return null;
        }

        return JsonUtils.unmarshalFromByte(data, LogPosition.class);
    }

    //存储destination的LogPosition信息
    public void persistLogPosition(String destination, LogPosition logPosition) {
        String path = ZookeeperPathUtils.getParsePath(destination);
        byte[] data = JsonUtils.marshalToByte(logPosition);
        try {
            zkClientx.writeData(path, data);
        } catch (ZkNoNodeException e) {
            zkClientx.createPersistent(path, data, true);
        }
    }

    // ================== setter / getter =================

    public void setZkClientx(ZkClientx zkClientx) {
        this.zkClientx = zkClientx;
    }

}
