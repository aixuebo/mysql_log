package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;

/**
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6
 * query的查询sql
 * MySQL新增了一个事务类型来输出ROW格式中原生的DML语句，即ROWS_QUERY_EVENT。
 */
public class RowsQueryLogEvent extends IgnorableLogEvent {

    private String rowsQuery;//记录查询sql

    public RowsQueryLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        final int postHeaderLen = descriptionEvent.postHeaderLen[header.type - 1];

        /*
         * m_rows_query length is stored using only one byte, but that length is
         * ignored and the complete query is read.
         */
        int offset = commonHeaderLen + postHeaderLen + 1;
        int len = buffer.limit() - offset;
        rowsQuery = buffer.getFullString(offset, len, LogBuffer.ISO_8859_1);
    }

    public String getRowsQuery() {
        return rowsQuery;
    }

}
