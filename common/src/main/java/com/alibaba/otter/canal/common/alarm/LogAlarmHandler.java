package com.alibaba.otter.canal.common.alarm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;

/**
 * 基于log的alarm机制实现
 * 基于log日志实现的报警机制
 * 
 * @author jianghang 2012-8-22 下午10:12:35
 * @version 1.0.0
 */
public class LogAlarmHandler extends AbstractCanalLifeCycle implements CanalAlarmHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogAlarmHandler.class);

    //当产生报警的时候记录日志即可
    public void sendAlarm(String destination, String msg) {
        logger.error("destination:{}[{}]", new Object[] { destination, msg });
    }

}
