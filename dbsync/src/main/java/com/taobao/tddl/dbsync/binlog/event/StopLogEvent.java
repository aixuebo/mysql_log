package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * Stop_log_event. The Post-Header and Body for this event type are empty; it
 * only has the Common-Header.
 * 不需要解析额外的内容,因此他就是使用正常公用的header,因此什么都不做
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class StopLogEvent extends LogEvent {

    public StopLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent description_event){
        super(header);
    }
}
