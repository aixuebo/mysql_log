package com.alibaba.otter.canal.parse.index;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * 接口组合-----记录每一个队列已经同步master的哪个日志位置了
 * 
 * @author jianghang 2012-7-7 上午10:02:02
 * @version 1.0.0
 * 用于服务器上存储每一个destination已经抓去到哪个位置了,当服务器挂了重启后,需要该信息
 */
public interface CanalLogPositionManager extends CanalLifeCycle {

    //获取该目的地的LogPosition信息
    LogPosition getLatestIndexBy(String destination);

    //存储destination的LogPosition信息
    void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException;
}
