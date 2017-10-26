package com.alibaba.otter.canal.store;

import java.util.List;

import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;

/**
 * store回收机制
 * 
 * @author jianghang 2012-8-8 下午12:57:36
 * @version 1.0.0
 * 找到最小的客户端确定的位置,用于删除该位置之前的数据
 */
public abstract class AbstractCanalStoreScavenge extends AbstractCanalLifeCycle implements CanalStoreScavenge {

    protected String           destination;//要监控的队列
    protected CanalMetaManager canalMetaManager;//server上的元数据中心
    protected boolean          onAck            = true;
    protected boolean          onFull           = false;
    protected boolean          onSchedule       = false;
    protected String           scavengeSchedule = null;

    public void scavenge() {
        Position position = getLatestAckPosition(destination);
        cleanUntil(position);
    }

    /**
     * 找出该destination中可被清理掉的position位置
     *
     * 即所有监听该destination队列的客户端集合,循环每一个客户端,找到每一个客户端ack确定的最小的位置
     * @param destination
     */
    private Position getLatestAckPosition(String destination) {
        List<ClientIdentity> clientIdentitys = canalMetaManager.listAllSubscribeInfo(destination);//所有监听该destination队列的客户端集合
        LogPosition result = null;
        if (!CollectionUtils.isEmpty(clientIdentitys)) {
            // 尝试找到一个最小的logPosition
            for (ClientIdentity clientIdentity : clientIdentitys) {//循环每一个客户端
                LogPosition position = (LogPosition) canalMetaManager.getCursor(clientIdentity);//找到每一个客户端ack确定的最小的位置
                if (position == null) {
                    continue;
                }

                if (result == null) {
                    result = position;
                } else {
                    result = min(result, position);
                }
            }
        }

        return result;
    }

    /**
     * 找出一个最小的position位置
     */
    private LogPosition min(LogPosition position1, LogPosition position2) {
        if (position1.getIdentity().equals(position2.getIdentity())) {
            // 首先根据文件进行比较
            if (position1.getPostion().getJournalName().compareTo(position2.getPostion().getJournalName()) < 0) {
                return position2;
            } else if (position1.getPostion().getJournalName().compareTo(position2.getPostion().getJournalName()) > 0) {
                return position1;
            } else {
                // 根据offest进行比较
                if (position1.getPostion().getPosition() < position2.getPostion().getPosition()) {
                    return position2;
                } else {
                    return position1;
                }
            }
        } else {
            // 不同的主备库，根据时间进行比较
            if (position1.getPostion().getTimestamp() < position2.getPostion().getTimestamp()) {
                return position2;
            } else {
                return position1;
            }
        }
    }

    public void setOnAck(boolean onAck) {
        this.onAck = onAck;
    }

    public void setOnFull(boolean onFull) {
        this.onFull = onFull;
    }

    public void setOnSchedule(boolean onSchedule) {
        this.onSchedule = onSchedule;
    }

    public String getScavengeSchedule() {
        return scavengeSchedule;
    }

    public void setScavengeSchedule(String scavengeSchedule) {
        this.scavengeSchedule = scavengeSchedule;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setCanalMetaManager(CanalMetaManager canalMetaManager) {
        this.canalMetaManager = canalMetaManager;
    }

}
