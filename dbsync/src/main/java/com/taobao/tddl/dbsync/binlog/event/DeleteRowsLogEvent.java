package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;

/**
 * Log row deletions. The event contain several delete rows for a table. Note
 * that each event contains only rows for one table.
 * 该事件删除的行,只能删除一个表的哪些行,不支持多表
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 * 该对象包含的行内容,表示删除的行信息
 */
public final class DeleteRowsLogEvent extends RowsLogEvent {

    public DeleteRowsLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
    }
}
