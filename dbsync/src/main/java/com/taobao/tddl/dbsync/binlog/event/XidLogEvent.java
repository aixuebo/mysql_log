package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * Logs xid of the transaction-to-be-committed in the 2pc protocol. Has no
 * meaning in replication, slaves ignore it.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 * 该事件没有任何意义,因此可以忽略他.主要表示二阶段协议日志提交了
 *
 * 一个8个字节的xid信息
 * 表示COMMIT 事件
 */
public final class XidLogEvent extends LogEvent {

    private final long xid;

    public XidLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        /* The Post-Header is empty. The Variable Data part begins immediately. */
        buffer.position(descriptionEvent.commonHeaderLen + descriptionEvent.postHeaderLen[XID_EVENT - 1]);
        xid = buffer.getLong64(); // !uint8korr
    }

    public final long getXid() {
        return xid;
    }
}
