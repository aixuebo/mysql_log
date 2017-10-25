package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;

/**
 * Event for the first block of file to be loaded, its only difference from
 * Append_block event is that this event creates or truncates existing file
 * before writing data.
 * 文件的第一个数据块被加载产生的事件
 * 他与AppendBlockLogEvent不同,是因为他会创建或者情况存在的文件,主要是第一个load的数据块时候,因此单独拿出来一个事件
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class BeginLoadQueryLogEvent extends AppendBlockLogEvent {

    public BeginLoadQueryLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
    }
}
