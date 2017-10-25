package com.alibaba.otter.canal.common.alarm;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * canal报警处理机制---此时是一个接口
 * 目前仅仅支持log日志的报警机制
 * @author jianghang 2012-8-22 下午10:08:56
 * @version 1.0.0
 */
public interface CanalAlarmHandler extends CanalLifeCycle {

    /**
     * 发送对应destination的报警
     * 
     * @param destination
     * @param msg
     * 即哪个destination发生报警了,报警内容是msg
     */
    void sendAlarm(String destination, String msg);
}
