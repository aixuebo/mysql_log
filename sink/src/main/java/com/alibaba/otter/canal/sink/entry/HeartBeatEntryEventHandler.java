package com.alibaba.otter.canal.sink.entry;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.sink.AbstractCanalEventDownStreamHandler;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 处理一下一下heartbeat数据,将心跳数据刨除掉
 * 
 * @author jianghang 2013-10-8 下午6:03:53
 * @since 1.0.12
 */
public class HeartBeatEntryEventHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

	//刨除心跳事件
    public List<Event> before(List<Event> events) {
        boolean existHeartBeat = false;//判断是否存在心跳事件
        for (Event event : events) {//循环所有事件,判断是否存在心跳事件
            if (event.getEntry().getEntryType() == EntryType.HEARTBEAT) {
                existHeartBeat = true;
            }
        }

        if (!existHeartBeat) {//说明不存在心跳事件,则返回整个事件即可
            return events;
        } else {
            // 目前heartbeat和其他事件是分离的，保险一点还是做一下检查处理
            List<Event> result = new ArrayList<Event>();
            for (Event event : events) {
                if (event.getEntry().getEntryType() != EntryType.HEARTBEAT) {
                    result.add(event);
                }
            }

            return result;
        }
    }

}
