package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * <pre>
 * Replication event to ensure to slave that master is alive.
 *   The event is originated by master's dump thread and sent straight to
 *   slave without being logged. Slave itself does not store it in relay log
 *   but rather uses a data for immediate checks and throws away the event.

 * 复制事件,确保从库还活着,是主库发送心跳的行为事件
 * 该事件通过master的线程直接发送到从库,不会记录日志,从库也不需要存储该日志,记录校验即可*
 *
 *   Two members of the class log_ident and Log_event::log_pos comprise 
 *   @see the event_coordinates instance. The coordinates that a heartbeat
 *   instance carries correspond to the last event master has sent from
 *   its binlog.
 * </pre>
    该类有两个属性,
 * 
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6
 */
public class HeartbeatLogEvent extends LogEvent {

    public static final int FN_REFLEN = 512; /* Max length of full path-name */
    private int             identLen;
    private String          logIdent;

    public HeartbeatLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        identLen = buffer.limit() - commonHeaderLen;
        if (identLen > FN_REFLEN - 1) {
            identLen = FN_REFLEN - 1;
        }

        logIdent = buffer.getFullString(commonHeaderLen, identLen, LogBuffer.ISO_8859_1);
    }

    public int getIdentLen() {
        return identLen;
    }

    public String getLogIdent() {
        return logIdent;
    }

}
