package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * Common-Header字段表示所有的事件都公用的头部信息,一共19个字节
 * Post-Header表示每一个事件类型有特殊的头部信息
 * body表示事件具体内容
 *
 * Post-Header和body在子类中讲解,该类只是讲解公共头文件
 *
 * The Common-Header, documented in the table @ref Table_common_header "below",
 * always has the same form and length within one version of MySQL. Each event
 * type specifies a format and length of the Post-Header. The length of the
 * Common-Header is the same for all events of the same type. The Body may be of
 * different format and length even for different events of the same type. The
 * binary formats of Post-Header and Body are documented separately in each
 * subclass. The binary format of Common-Header is as follows.
 *
 * 格式:一共占用19个byte表示的头文件说明:
 * 4个字节,timestamp时间戳,单位是秒----当查询开始时候的时间
 * 1个字节,type事件类型,
 * 4个字节,server_id,即创建该事件的server是哪个server
 * 4个字节,total_size 事件的总字节大小,即(Common-Header+Post-Header+Body)
 * 4个字节,master_position,表示master上,下一个事件在binlog日志中要写入的位置
 relay-log日志记录的是从服务器I/O线程将主服务器的二进制日志读取过来记录到从服务器本地文件，然后SQL线程会读取relay-log日志的内容并应用到从服务器
 binlog不是relay-log日志,因此该位置就是下一个事件在master上要写入到binlog日志的位置偏移量
 而在relay-log日志中.该位置是下一个事件在master的binlog上的偏移量,不是relay-log的偏移量
 有些绕,详细参见视频
  2个字节,flags,参见 Log_event::flags,表示事件的特殊用法

 * </tr>
 * <tr>
 * <td>flags</td>
 * <td>2 byte bitfield</td>
 * <td>See Log_event::flags.</td>
 * </tr>
 * </table>
 *
 * <table>
 * <caption>Common-Header</caption>
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>timestamp</td>
 * <td>4 byte unsigned integer</td>
 * <td>The time when the query started, in seconds since 1970.</td>
 * </tr>
 * <tr>
 * <td>type</td>
 * <td>1 byte enumeration</td>
 * <td>See enum #Log_event_type.</td>
 * </tr>
 * <tr>
 * <td>server_id</td>
 * <td>4 byte unsigned integer</td>
 * <td>Server ID of the server that created the event.</td>
 * </tr>
 * <tr>
 * <td>total_size</td>
 * <td>4 byte unsigned integer</td>
 * <td>The total size of this event, in bytes. In other words, this is the sum
 * of the sizes of Common-Header, Post-Header, and Body.</td>
 * </tr>
 * <tr>
 * <td>master_position</td>
 * <td>4 byte unsigned integer</td>
 * <td>The position of the next event in the master binary log, in bytes from
 * the beginning of the file. In a binlog that is not a relay log, this is just
 * the position of the next event, in bytes from the beginning of the file. In a
 * relay log, this is the position of the next event in the master's binlog.</td>
 * </tr>
 * <tr>
 * <td>flags</td>
 * <td>2 byte bitfield</td>
 * <td>See Log_event::flags.</td>
 * </tr>
 * </table>
 * Summing up the numbers above, we see that the total size of the common header
 * is 19 bytes.
 * 
 * @see mysql-5.1.60/sql/log_event.cc
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class LogHeader {

    protected final int type;//事件类型

    /**
     * The offset in the log where this event originally appeared (it is
     * preserved in relay logs, making SHOW SLAVE STATUS able to print
     * coordinates of the event in the master's binlog). Note: when a
     * transaction is written by the master to its binlog (wrapped in
     * BEGIN/COMMIT) the log_pos of all the queries it contains is the one of
     * the BEGIN (this way, when one does SHOW SLAVE STATUS it sees the offset
     * of the BEGIN, which is logical as rollback may occur), except the COMMIT
     * query which has its real offset.
     * 下一个事件在master的binlog的开始偏移量,方便下一次获取
     */
    protected long      logPos;

    /**
     * Timestamp on the master(for debugging and replication of
     * NOW()/TIMESTAMP). It is important for queries and LOAD DATA INFILE. This
     * is set at the event's creation time, except for Query and Load (et al.)
     * events where this is set at the query's execution time, which guarantees
     * good replication (otherwise, we could have a query and its event with
     * different timestamps).
     * 4个字节的时间戳---事件在master上产生的时间戳
     */
    protected long      when;

    /** Number of bytes written by write() function
     * 事件的总长度,包含头+特殊头+body
     **/
    protected int       eventLen;

    /**
     * The master's server id (is preserved in the relay log; used to prevent
     * from infinite loops in circular replication).
     * master产生该事件的服务器ID
     */
    protected long      serverId;

    /**
     * Some 16 flags. See the definitions above for LOG_EVENT_TIME_F,
     * LOG_EVENT_FORCED_ROTATE_F, LOG_EVENT_THREAD_SPECIFIC_F, and
     * LOG_EVENT_SUPPRESS_USE_F for notes.
     * 事件的特殊表示
     */
    protected int       flags;

    /**
     * The value is set by caller of FD constructor and
     * Log_event::write_header() for the rest. In the FD case it's propagated
     * into the last byte of post_header_len[] at FD::write(). On the slave side
     * the value is assigned from post_header_len[last] of the last seen FD
     * event.
     * 该值表示校验和算法
     */
    protected int       checksumAlg;
    /**
     * Placeholder for event checksum while writing to binlog.
     * binlog解析的事件中校验和的内容,该内容是binlog的master传递过来的
     */
    protected long      crc;        // ha_checksum

    /* for Start_event_v3 */
    public LogHeader(final int type){
        this.type = type;
    }

    //此时的buffer就是从事件的头开始计算的,已经没有了魔
    public LogHeader(LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        when = buffer.getUint32();//4个字节的时间戳
        type = buffer.getUint8(); // LogEvent.EVENT_TYPE_OFFSET; 1个字节的事件类型
        serverId = buffer.getUint32(); // LogEvent.SERVER_ID_OFFSET;4个字节的事件产生的服务器ID
        eventLen = (int) buffer.getUint32(); // LogEvent.EVENT_LEN_OFFSET;事件的总长度

        if (descriptionEvent.binlogVersion == 1) {//说明版本是1,日志版本是1我们先不考虑,因为我们仅仅考虑版本是4的日志
            logPos = 0;
            flags = 0;
            return;
        }

        /* 4.0 or newer 日志格式是4.0以后的版本 */
        logPos = buffer.getUint32(); // LogEvent.LOG_POS_OFFSET 获取下一个事件在master的binlog中的起始位置
        /*
         * If the log is 4.0 (so here it can only be a 4.0 relay log read by the
         * SQL thread or a 4.0 master binlog read by the I/O thread), log_pos is
         * the beginning of the event: we transform it into the end of the
         * event, which is more useful. But how do you know that the log is 4.0:
         * you know it if description_event is version 3 *and* you are not
         * reading a Format_desc (remember that mysqlbinlog starts by assuming
         * that 5.0 logs are in 4.0 format, until it finds a Format_desc).
         * 针对版本是3的时候进行处理,基本上新版本都是4,因此这部分代码可以忽略
         */
        if (descriptionEvent.binlogVersion == 3 && type < LogEvent.FORMAT_DESCRIPTION_EVENT && logPos != 0) {
            /*
             * If log_pos=0, don't change it. log_pos==0 is a marker to mean
             * "don't change rli->group_master_log_pos" (see
             * inc_group_relay_log_pos()). As it is unreal log_pos, adding the
             * event len's is nonsense. For example, a fake Rotate event should
             * not have its log_pos (which is 0) changed or it will modify
             * Exec_master_log_pos in SHOW SLAVE STATUS, displaying a nonsense
             * value of (a non-zero offset which does not exist in the master's
             * binlog, so which will cause problems if the user uses this value
             * in CHANGE MASTER).
             */
            logPos += eventLen; /* purecov: inspected */
        }

        flags = buffer.getUint16(); // LogEvent.FLAGS_OFFSET  2个字节,获取附加的flag信息
        if ((type == LogEvent.FORMAT_DESCRIPTION_EVENT) || (type == LogEvent.ROTATE_EVENT)) {
            /*
             * These events always have a header which stops here (i.e. their
             * header is FROZEN).
             */
            /*
             * Initialization to zero of all other Log_event members as they're
             * not specified. Currently there are no such members; in the future
             * there will be an event UID (but Format_description and Rotate
             * don't need this UID, as they are not propagated through
             * --log-slave-updates (remember the UID is used to not play a query
             * twice when you have two masters which are slaves of a 3rd
             * master). Then we are done.
             */
            if (type == LogEvent.FORMAT_DESCRIPTION_EVENT) {//说明是格式描述事件
                int commonHeaderLen = buffer.getUint8(FormatDescriptionLogEvent.LOG_EVENT_MINIMAL_HEADER_LEN //头的最小长度
                                                      + FormatDescriptionLogEvent.ST_COMMON_HEADER_LEN_OFFSET);//定位到19+2+50+4位置,读取一个字节的头的长度
                buffer.position(commonHeaderLen + FormatDescriptionLogEvent.ST_SERVER_VER_OFFSET);//读取2个byte
                String serverVersion = buffer.getFixString(FormatDescriptionLogEvent.ST_SERVER_VER_LEN); // ST_SERVER_VER_OFFSET
                int versionSplit[] = new int[] { 0, 0, 0 };
                FormatDescriptionLogEvent.doServerVersionSplit(serverVersion, versionSplit);//计算版本号
                checksumAlg = LogEvent.BINLOG_CHECKSUM_ALG_UNDEF;//默认校验和算法
                if (FormatDescriptionLogEvent.versionProduct(versionSplit) >= FormatDescriptionLogEvent.checksumVersionProduct) {
                    buffer.position(eventLen - LogEvent.BINLOG_CHECKSUM_LEN - LogEvent.BINLOG_CHECKSUM_ALG_DESC_LEN);
                    checksumAlg = buffer.getUint8();//获取一个byte的字节的校验和算法
                }

                processCheckSum(buffer);
            }
            return;
        }

        /*
         * CRC verification by SQL and Show-Binlog-Events master side. The
         * caller has to provide @description_event->checksum_alg to be the last
         * seen FD's (A) descriptor. If event is FD the descriptor is in it.
         * Notice, FD of the binlog can be only in one instance and therefore
         * Show-Binlog-Events executing master side thread needs just to know
         * the only FD's (A) value - whereas RL can contain more. In the RL
         * case, the alg is kept in FD_e (@description_event) which is reset to
         * the newer read-out event after its execution with possibly new alg
         * descriptor. Therefore in a typical sequence of RL: {FD_s^0, FD_m,
         * E_m^1} E_m^1 will be verified with (A) of FD_m. See legends
         * definition on MYSQL_BIN_LOG::relay_log_checksum_alg docs lines
         * (log.h). Notice, a pre-checksum FD version forces alg :=
         * BINLOG_CHECKSUM_ALG_UNDEF.
         */
        checksumAlg = descriptionEvent.getHeader().checksumAlg; // fetch
                                                                // checksum alg
        processCheckSum(buffer);
        /* otherwise, go on with reading the header from buf (nothing now) */
    }

    /**
     * The different types of log events.
     */
    public final int getType() {
        return type;
    }

    /**
     * The position of the next event in the master binary log, in bytes from
     * the beginning of the file. In a binlog that is not a relay log, this is
     * just the position of the next event, in bytes from the beginning of the
     * file. In a relay log, this is the position of the next event in the
     * master's binlog.
     */
    public final long getLogPos() {
        return logPos;
    }

    /**
     * The total size of this event, in bytes. In other words, this is the sum
     * of the sizes of Common-Header, Post-Header, and Body.
     */
    public final int getEventLen() {
        return eventLen;
    }

    /**
     * The time when the query started, in seconds since 1970.
     */
    public final long getWhen() {
        return when;
    }

    /**
     * Server ID of the server that created the event.
     */
    public final long getServerId() {
        return serverId;
    }

    /**
     * Some 16 flags. See the definitions above for LOG_EVENT_TIME_F,
     * LOG_EVENT_FORCED_ROTATE_F, LOG_EVENT_THREAD_SPECIFIC_F, and
     * LOG_EVENT_SUPPRESS_USE_F for notes.
     */
    public final int getFlags() {
        return flags;
    }

    public long getCrc() {
        return crc;
    }

    public int getChecksumAlg() {
        return checksumAlg;
    }

    private void processCheckSum(LogBuffer buffer) {
        if (checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_OFF && checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_UNDEF) {//说明要解析校验和
            crc = buffer.getUint32(eventLen - LogEvent.BINLOG_CHECKSUM_LEN);//事件总字节数-校验和内容所占用的字节数,即校验和内容的字节位置,读取一个int表示校验和的值
        }
    }
}
