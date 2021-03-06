package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.CanalHASwitchable;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.HeartBeatCallback;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.BinlogFormat;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.BinlogImage;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * 基于向mysql server复制binlog实现
 * 
 * <pre>
 * 1. 自身不控制mysql主备切换，由ha机制来控制. 比如接入tddl/cobar/自身心跳包成功率
 * 2. 切换机制
 * </pre>
 * 
 * @author jianghang 2012-6-21 下午04:06:32
 * @version 1.0.0
 */
public class MysqlEventParser extends AbstractMysqlEventParser implements CanalEventParser, CanalHASwitchable {

    private CanalHAController  haController                      = null;

    private int                defaultConnectionTimeoutInSeconds = 30;       // sotimeout
    private int                receiveBufferSize                 = 64 * 1024;
    private int                sendBufferSize                    = 64 * 1024;
    // 数据库信息
    private AuthenticationInfo masterInfo;                                   // 主库
    private AuthenticationInfo standbyInfo;                                  // 备库
    // binlog信息
    private EntryPosition      masterPosition;
    private EntryPosition      standbyPosition;
    private long               slaveId;                                      // 链接到mysql的slave
    // 心跳检查信息
    private String             detectingSQL;                                 // 心跳sql  定期发送给master的sql去检查
    private MysqlConnection    metaConnection;                               // 查询meta信息的链接
    private TableMetaCache     tableMetaCache;                               // 对应meta
                                                                              // cache
    private int                fallbackIntervalInSeconds         = 60;       // 切换回退时间
    private BinlogFormat[]     supportBinlogFormats;                         // 支持的binlogFormat,如果设置会执行强校验
    private BinlogImage[]      supportBinlogImages;                          // 支持的binlogImage,如果设置会执行强校验

    // 心跳检查

    protected ErosaConnection buildErosaConnection() {
        return buildMysqlConnection(this.runningInfo);
    }

    //获取dump mysql的binlog前,对一些元数据类需要初始化等操作
    protected void preDump(ErosaConnection connection) {
        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        if (binlogParser != null && binlogParser instanceof LogEventConvert) {
            metaConnection = (MysqlConnection) connection.fork();
            try {
                metaConnection.connect();
            } catch (IOException e) {
                throw new CanalParseException(e);
            }

            if (supportBinlogFormats != null && supportBinlogFormats.length > 0) {
                BinlogFormat format = ((MysqlConnection) metaConnection).getBinlogFormat();
                boolean found = false;
                for (BinlogFormat supportFormat : supportBinlogFormats) {
                    if (supportFormat != null && format == supportFormat) {
                        found = true;
                    }
                }

                if (!found) {
                    throw new CanalParseException("Unsupported BinlogFormat " + format);
                }
            }

            if (supportBinlogImages != null && supportBinlogImages.length > 0) {
                BinlogImage image = ((MysqlConnection) metaConnection).getBinlogImage();
                boolean found = false;
                for (BinlogImage supportImage : supportBinlogImages) {
                    if (supportImage != null && image == supportImage) {
                        found = true;
                    }
                }

                if (!found) {
                    throw new CanalParseException("Unsupported BinlogImage " + image);
                }
            }

            tableMetaCache = new TableMetaCache(metaConnection);
            ((LogEventConvert) binlogParser).setTableMetaCache(tableMetaCache);
        }
    }

    protected void afterDump(ErosaConnection connection) {
        super.afterDump(connection);

        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", metaConnection.getConnector()
                    .getAddress(), e);
            }
        }
    }

    public void start() throws CanalParseException {
        if (runningInfo == null) { // 第一次链接主库
            runningInfo = masterInfo;
        }

        super.start();
    }

    public void stop() throws CanalParseException {
        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", metaConnection.getConnector()
                    .getAddress(), e);
            }
        }

        if (tableMetaCache != null) {
            tableMetaCache.clearTableMeta();
        }

        super.stop();
    }

    protected TimerTask buildHeartBeatTimeTask(ErosaConnection connection) {
        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        // 开始mysql心跳sql
        if (detectingEnable && StringUtils.isNotBlank(detectingSQL)) {
            return new MysqlDetectingTimeTask((MysqlConnection) connection.fork());
        } else {
            return super.buildHeartBeatTimeTask(connection);
        }

    }

    protected void stopHeartBeat() {
        super.stopHeartBeat();

        if (heartBeatTimerTask != null) {

            MysqlConnection mysqlConnection = ((MysqlDetectingTimeTask) heartBeatTimerTask).getMysqlConnection();
            try {
                mysqlConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect heartbeat connection for address:{}", mysqlConnection.getConnector()
                    .getAddress(), e);
            }
        }
    }

    /**
     * 心跳信息
     * 
     * @author jianghang 2012-7-6 下午02:50:15
     * @version 1.0.0
     */
    class MysqlDetectingTimeTask extends TimerTask {

        private boolean         reconnect = false;
        private MysqlConnection mysqlConnection;

        public MysqlDetectingTimeTask(MysqlConnection mysqlConnection){
            this.mysqlConnection = mysqlConnection;
        }

        public void run() {
            try {
                if (reconnect) {
                    reconnect = false;
                    mysqlConnection.reconnect();
                } else if (!mysqlConnection.isConnected()) {
                    mysqlConnection.connect();
                }
                Long startTime = System.currentTimeMillis();

                // 可能心跳sql为select 1  查看该sql是什么类型的sql,执行不同的sql方式
                if (StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "select")
                    || StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "show")
                    || StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "explain")
                    || StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "desc")) {
                    mysqlConnection.query(detectingSQL);
                } else {
                    mysqlConnection.update(detectingSQL);
                }

                Long costTime = System.currentTimeMillis() - startTime;//记录心跳过程的查询耗时
                if (haController != null && haController instanceof HeartBeatCallback) {
                    ((HeartBeatCallback) haController).onSuccess(costTime);
                }
            } catch (SocketTimeoutException e) {
                if (haController != null && haController instanceof HeartBeatCallback) {
                    ((HeartBeatCallback) haController).onFailed(e);
                }
                reconnect = true;
                logger.warn("connect failed by " + ExceptionUtils.getStackTrace(e));
            } catch (IOException e) {
                if (haController != null && haController instanceof HeartBeatCallback) {
                    ((HeartBeatCallback) haController).onFailed(e);
                }
                reconnect = true;
                logger.warn("connect failed by " + ExceptionUtils.getStackTrace(e));
            } catch (Throwable e) {
                if (haController != null && haController instanceof HeartBeatCallback) {
                    ((HeartBeatCallback) haController).onFailed(e);
                }
                reconnect = true;
                logger.warn("connect failed by " + ExceptionUtils.getStackTrace(e));
            }

        }

        public MysqlConnection getMysqlConnection() {
            return mysqlConnection;
        }
    }

    // 处理主备切换的逻辑
    public void doSwitch() {
        AuthenticationInfo newRunningInfo = (runningInfo.equals(masterInfo) ? standbyInfo : masterInfo);
        this.doSwitch(newRunningInfo);
    }

    //参数是切换的节点
    public void doSwitch(AuthenticationInfo newRunningInfo) {
        // 1. 需要停止当前正在复制的过程
        // 2. 找到新的position点
        // 3. 重新建立链接，开始复制数据
        // 切换ip
        String alarmMessage = null;

        if (this.runningInfo.equals(newRunningInfo)) {//说明没有切换成功
            alarmMessage = "same runingInfo switch again : " + runningInfo.getAddress().toString();
            logger.warn(alarmMessage);
            return;
        }

        if (newRunningInfo == null) {
            alarmMessage = "no standby config, just do nothing, will continue try:"
                           + runningInfo.getAddress().toString();
            logger.warn(alarmMessage);
            sendAlarm(destination, alarmMessage);
            return;
        } else {
            stop();//停止当前服务
            alarmMessage = "try to ha switch, old:" + runningInfo.getAddress().toString() + ", new:"
                           + newRunningInfo.getAddress().toString();
            logger.warn(alarmMessage);
            sendAlarm(destination, alarmMessage);
            runningInfo = newRunningInfo;
            start();//重新开启一个mysql服务器去读取binlog
        }
    }

    // =================== helper method =================

    private MysqlConnection buildMysqlConnection(AuthenticationInfo runningInfo) {
        MysqlConnection connection = new MysqlConnection(runningInfo.getAddress(),
            runningInfo.getUsername(),
            runningInfo.getPassword(),
            connectionCharsetNumber,
            runningInfo.getDefaultDatabaseName());
        connection.getConnector().setReceiveBufferSize(receiveBufferSize);
        connection.getConnector().setSendBufferSize(sendBufferSize);
        connection.getConnector().setSoTimeout(defaultConnectionTimeoutInSeconds * 1000);
        connection.setCharset(connectionCharset);
        connection.setSlaveId(this.slaveId);
        return connection;
    }

    protected EntryPosition findStartPosition(ErosaConnection connection) throws IOException {
        EntryPosition startPosition = findStartPositionInternal(connection);
        if (needTransactionPosition.get()) {
            logger.warn("prepare to find last position : {}", startPosition.toString());
            Long preTransactionStartPosition = findTransactionBeginPosition(connection, startPosition);//找到事务的开始位置
            if (!preTransactionStartPosition.equals(startPosition.getPosition())) {
                logger.warn("find new start Transaction Position , old : {} , new : {}",
                    startPosition.getPosition(),
                    preTransactionStartPosition);
                startPosition.setPosition(preTransactionStartPosition);
            }
            needTransactionPosition.compareAndSet(true, false);
        }
        return startPosition;
    }
    
	protected EntryPosition findEndPosition(ErosaConnection connection) throws IOException {
		MysqlConnection mysqlConnection = (MysqlConnection) connection;
		 EntryPosition endPosition = findEndPosition(mysqlConnection);
		return endPosition;
	}

    protected EntryPosition findStartPositionInternal(ErosaConnection connection) {
        MysqlConnection mysqlConnection = (MysqlConnection) connection;
        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);//获取保存的接收到哪个位置了
        if (logPosition == null) {// 找不到历史成功记录
            //查看当前节点是master还是slave,从而从不同的节点上获取设置的同步点开始同步数据
            EntryPosition entryPosition = null;
            if (masterInfo != null && mysqlConnection.getConnector().getAddress().equals(masterInfo.getAddress())) {//说明是master节点
                entryPosition = masterPosition;//因此从设置的master节点上的点开始同步
            } else if (standbyInfo != null
                       && mysqlConnection.getConnector().getAddress().equals(standbyInfo.getAddress())) {
                entryPosition = standbyPosition;
            }

            //此时说明没有设置要同步的master或者slave的同步点,因此查找最新的点
            if (entryPosition == null) {
                entryPosition = findEndPosition(mysqlConnection); // 默认从当前最后一个位置进行消费
            }

            // 判断一下是否需要按时间订阅
            if (StringUtils.isEmpty(entryPosition.getJournalName())) {//说明没有指定文件名
                // 如果没有指定binlogName，尝试按照timestamp进行查找
                if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {//按照时间查找
                    logger.warn("prepare to find start position {}:{}:{}",
                        new Object[] { "", "", entryPosition.getTimestamp() });
                    return findByStartTimeStamp(mysqlConnection, entryPosition.getTimestamp());
                } else {//获取最新的位置
                    logger.warn("prepare to find start position just show master status");
                    return findEndPosition(mysqlConnection); // 默认从当前最后一个位置进行消费
                }
            } else {//说明设置了文件名
                if (entryPosition.getPosition() != null && entryPosition.getPosition() > 0L) {
                    // 如果指定binlogName + offest，直接返回
                    logger.warn("prepare to find start position {}:{}:{}",
                        new Object[] { entryPosition.getJournalName(), entryPosition.getPosition(), "" });
                    return entryPosition;
                } else {
                    EntryPosition specificLogFilePosition = null;
                    if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                        // 如果指定binlogName +
                        // timestamp，但没有指定对应的offest，尝试根据时间找一下offest
                        EntryPosition endPosition = findEndPosition(mysqlConnection);
                        if (endPosition != null) {
                            logger.warn("prepare to find start position {}:{}:{}",
                                new Object[] { entryPosition.getJournalName(), "", entryPosition.getTimestamp() });
                            specificLogFilePosition = findAsPerTimestampInSpecificLogFile(mysqlConnection,
                                entryPosition.getTimestamp(),
                                endPosition,
                                entryPosition.getJournalName());
                        }
                    }

                    if (specificLogFilePosition == null) {
                        // position不存在，从文件头开始
                        entryPosition.setPosition(BINLOG_START_OFFEST);
                        return entryPosition;
                    } else {
                        return specificLogFilePosition;
                    }
                }
            }
        } else {//说明找到保存的位置
            if (logPosition.getIdentity().getSourceAddress().equals(mysqlConnection.getConnector().getAddress())) {//查看保存的地址服务器和现在链接的服务器是否相同
                logger.warn("prepare to find start position just last position");
                return logPosition.getPostion();
            } else {
                // 针对切换的情况，考虑回退时间
                long newStartTimestamp = logPosition.getPostion().getTimestamp() - fallbackIntervalInSeconds * 1000;
                logger.warn("prepare to find start position by switch {}:{}:{}", new Object[] { "", "",
                        logPosition.getPostion().getTimestamp() });
                return findByStartTimeStamp(mysqlConnection, newStartTimestamp);
            }
        }
    }

    // 根据想要的position，可能这个position对应的记录为rowdata，需要找到事务头，避免丢数据
    // 主要考虑一个事务执行时间可能会几秒种，如果仅仅按照timestamp相同，则可能会丢失事务的前半部分数据
    //获取给定参数的entryPosition对应的事务的位置
    private Long findTransactionBeginPosition(ErosaConnection mysqlConnection, final EntryPosition entryPosition)
                                                                                                                 throws IOException {
        // 尝试找到一个合适的位置
        final AtomicBoolean reDump = new AtomicBoolean(false);
        mysqlConnection.reconnect();
        mysqlConnection.seek(entryPosition.getJournalName(), entryPosition.getPosition(), new SinkFunction<LogEvent>() {

            private LogPosition lastPosition;

            public boolean sink(LogEvent event) {
                try {
                    CanalEntry.Entry entry = parseAndProfilingIfNecessary(event);//解析该事件
                    if (entry == null) {
                        return true;
                    }

                    // 直接查询第一条业务数据，确认是否为事务Begin/End
                    if (CanalEntry.EntryType.TRANSACTIONBEGIN == entry.getEntryType()
                        || CanalEntry.EntryType.TRANSACTIONEND == entry.getEntryType()) {
                        lastPosition = buildLastPosition(entry);
                        return false;
                    } else {
                        reDump.set(true);
                        lastPosition = buildLastPosition(entry);
                        return false;
                    }
                } catch (Exception e) {
                    // 上一次记录的poistion可能为一条update/insert/delete变更事件，直接进行dump的话，会缺少tableMap事件，导致tableId未进行解析
                    processError(e, lastPosition, entryPosition.getJournalName(), entryPosition.getPosition());
                    reDump.set(true);
                    return false;
                }
            }
        });
        // 针对开始的第一条为非Begin记录，需要从该binlog扫描
        if (reDump.get()) {
            final AtomicLong preTransactionStartPosition = new AtomicLong(0L);
            mysqlConnection.reconnect();
            mysqlConnection.seek(entryPosition.getJournalName(), 4L, new SinkFunction<LogEvent>() {//从文件头开始扫描

                private LogPosition lastPosition;

                public boolean sink(LogEvent event) {
                    try {
                        CanalEntry.Entry entry = parseAndProfilingIfNecessary(event);//解析事件
                        if (entry == null) {
                            return true;
                        }

                        // 直接查询第一条业务数据，确认是否为事务Begin
                        // 记录一下transaction begin position
                        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                            && entry.getHeader().getLogfileOffset() < entryPosition.getPosition()) {
                            preTransactionStartPosition.set(entry.getHeader().getLogfileOffset());//不断的设置最新的事务开始位置
                        }

                        if (entry.getHeader().getLogfileOffset() >= entryPosition.getPosition()) {//说明事务已经超出范围了,结束
                            return false;// 退出
                        }

                        lastPosition = buildLastPosition(entry);
                    } catch (Exception e) {
                        processError(e, lastPosition, entryPosition.getJournalName(), entryPosition.getPosition());
                        return false;
                    }

                    return running;
                }
            });

            // 判断一下找到的最接近position的事务头的位置
            if (preTransactionStartPosition.get() > entryPosition.getPosition()) {
                logger.error("preTransactionEndPosition greater than startPosition from zk or localconf, maybe lost data");
                throw new CanalParseException("preTransactionStartPosition greater than startPosition from zk or localconf, maybe lost data");
            }
            return preTransactionStartPosition.get();//返回事务的位置
        } else {
            return entryPosition.getPosition();
        }
    }

    // 根据时间查找binlog位置
    private EntryPosition findByStartTimeStamp(MysqlConnection mysqlConnection, Long startTimestamp) {
        //获取目前binlog文件的所有日志中开始的位置和最后的位置,以及开始的文件名字 以及结束的文件名字
        EntryPosition endPosition = findEndPosition(mysqlConnection);
        EntryPosition startPosition = findStartPosition(mysqlConnection);
        String maxBinlogFileName = endPosition.getJournalName();
        String minBinlogFileName = startPosition.getJournalName();
        logger.info("show master status to set search end condition:{} ", endPosition);
        String startSearchBinlogFile = endPosition.getJournalName();//从最后一个文件开始查找
        boolean shouldBreak = false;
        while (running && !shouldBreak) {
            try {
                EntryPosition entryPosition = findAsPerTimestampInSpecificLogFile(mysqlConnection,
                    startTimestamp,
                    endPosition,
                    startSearchBinlogFile);//查获文件内对应的时间戳
                if (entryPosition == null) {
                    if (StringUtils.equalsIgnoreCase(minBinlogFileName, startSearchBinlogFile)) {
                        // 已经找到最早的一个binlog，没必要往前找了
                        shouldBreak = true;
                        logger.warn("Didn't find the corresponding binlog files from {} to {}",
                            minBinlogFileName,
                            maxBinlogFileName);//说明没有找到开始时间点
                    } else {
                        // 继续往前找
                        int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));//获取此时的binlog文件序号
                        if (binlogSeqNum <= 1) {
                            logger.warn("Didn't find the corresponding binlog files");
                            shouldBreak = true;
                        } else {
                            int nextBinlogSeqNum = binlogSeqNum - 1;//获取上一个binlog文件
                            String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
                                startSearchBinlogFile.indexOf(".") + 1);
                            String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                            startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                        }
                    }
                } else {
                    logger.info("found and return:{} in findByStartTimeStamp operation.", entryPosition);
                    return entryPosition;
                }
            } catch (Exception e) {
                logger.warn("the binlogfile:{} doesn't exist, to continue to search the next binlogfile , caused by {}",
                    startSearchBinlogFile,
                    ExceptionUtils.getFullStackTrace(e));
                int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                if (binlogSeqNum <= 1) {
                    logger.warn("Didn't find the corresponding binlog files");
                    shouldBreak = true;
                } else {
                    int nextBinlogSeqNum = binlogSeqNum - 1;
                    String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
                        startSearchBinlogFile.indexOf(".") + 1);
                    String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                    startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                }
            }
        }
        // 找不到
        return null;
    }

    /**
     * 查询当前的binlog位置
     * 返回此时master的binlog写到哪个位置了
     */
    private EntryPosition findEndPosition(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show master status");
            List<String> fields = packet.getFieldValues();
            if (CollectionUtils.isEmpty(fields)) {
                throw new CanalParseException("command : 'show master status' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
            }
            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));//获取binlog文件以及POSITION
            return endPosition;
        } catch (IOException e) {
            throw new CanalParseException("command : 'show master status' has an error!", e);
        }
    }

    /**
     * 查询当前的binlog日志存在的第一个位置
     */
    private EntryPosition findStartPosition(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show binlog events limit 1");
            List<String> fields = packet.getFieldValues();
            if (CollectionUtils.isEmpty(fields)) {
                throw new CanalParseException("command : 'show binlog events limit 1' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
            }
            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
            return endPosition;
        } catch (IOException e) {
            throw new CanalParseException("command : 'show binlog events limit 1' has an error!", e);
        }

    }

    /**
     * 查询当前的slave视图的binlog位置
     * 通过下面四个属性,可以知道该slave读取到master哪个binlog文件,哪个偏移量位置了,以及master的host和port是什么
     *
     */
    @SuppressWarnings("unused")
    private SlaveEntryPosition findSlavePosition(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show slave status");
            List<FieldPacket> names = packet.getFieldDescriptors();//结果集的schema信息
            List<String> fields = packet.getFieldValues();//结果集
            if (CollectionUtils.isEmpty(fields)) {//说明没有结果集
                return null;
            }

            int i = 0;
            Map<String, String> maps = new HashMap<String, String>(names.size(), 1f);
            for (FieldPacket name : names) {
                maps.put(name.getName(), fields.get(i));
                i++;
            }

            String errno = maps.get("Last_Errno");
            String slaveIORunning = maps.get("Slave_IO_Running"); // Slave_SQL_Running
            String slaveSQLRunning = maps.get("Slave_SQL_Running"); // Slave_SQL_Running
            if ((!"0".equals(errno)) || (!"Yes".equalsIgnoreCase(slaveIORunning))
                || (!"Yes".equalsIgnoreCase(slaveSQLRunning))) {
                logger.warn("Ignoring failed slave: " + mysqlConnection.getConnector().getAddress() + ", Last_Errno = "
                            + errno + ", Slave_IO_Running = " + slaveIORunning + ", Slave_SQL_Running = "
                            + slaveSQLRunning);
                return null;
            }

            String masterHost = maps.get("Master_Host");
            String masterPort = maps.get("Master_Port");
            String binlog = maps.get("Master_Log_File");
            String position = maps.get("Exec_Master_Log_Pos");
            return new SlaveEntryPosition(binlog, Long.valueOf(position), masterHost, masterPort);
        } catch (IOException e) {
            logger.error("find slave position error", e);
        }

        return null;
    }

    /**
     * 根据给定的时间戳，在指定的binlog中找到最接近于该时间戳(必须是小于时间戳)的一个事务起始位置。
     * 针对最后一个binlog会给定endPosition，避免无尽的查询
     *
     * 在方法表示在一个指定的binlog文件中,通过时间戳去找到最接近该时间戳的数据的位置,前提是在一个事务中,即从事务头开始查找
     */
    private EntryPosition findAsPerTimestampInSpecificLogFile(MysqlConnection mysqlConnection,
                                                              final Long startTimestamp,//要查找的时间戳
                                                              final EntryPosition endPosition,//此时binlog最新的位置
                                                              final String searchBinlogFile) {//要查找的文件

        final LogPosition logPosition = new LogPosition();
        try {
            mysqlConnection.reconnect();
            // 开始遍历文件
            mysqlConnection.seek(searchBinlogFile, 4L, new SinkFunction<LogEvent>() {//从文件开头开始查询

                private LogPosition lastPosition;

                public boolean sink(LogEvent event) {
                    EntryPosition entryPosition = null;
                    try {
                        CanalEntry.Entry entry = parseAndProfilingIfNecessary(event);//解析事件
                        if (entry == null) {
                            return true;
                        }

                        String logfilename = entry.getHeader().getLogfileName();
                        Long logfileoffset = entry.getHeader().getLogfileOffset();
                        Long logposTimestamp = entry.getHeader().getExecuteTime();

                        if (CanalEntry.EntryType.TRANSACTIONBEGIN.equals(entry.getEntryType())
                            || CanalEntry.EntryType.TRANSACTIONEND.equals(entry.getEntryType())) {
                            logger.debug("compare exit condition:{},{},{}, startTimestamp={}...", new Object[] {
                                    logfilename, logfileoffset, logposTimestamp, startTimestamp });
                            // 事务头和尾寻找第一条记录时间戳，如果最小的一条记录都不满足条件，可直接退出
                            if (logposTimestamp >= startTimestamp) {//说明已经超过了要查找的时间戳了,直接返回
                                return false;
                            }
                        }

                        if (StringUtils.equals(endPosition.getJournalName(), logfilename)
                            && endPosition.getPosition() <= (logfileoffset + event.getEventLen())) {//说明超过了最后一条binlog了,因此也要返回false,因为binlog在不断的写入数据,因此可能存在超过我们参数对应的最后一条数据的时候
                            return false;
                        }

                        // 记录一下上一个事务结束的位置，即下一个事务的position
                        // position = current +
                        // data.length，代表该事务的下一条offest，避免多余的事务重复
                        if (CanalEntry.EntryType.TRANSACTIONEND.equals(entry.getEntryType())) {
                            entryPosition = new EntryPosition(logfilename,
                                logfileoffset + event.getEventLen(),//记录下一条的开始位置
                                logposTimestamp);
                            logger.debug("set {} to be pending start position before finding another proper one...",
                                entryPosition);
                            logPosition.setPostion(entryPosition);
                        } else if (CanalEntry.EntryType.TRANSACTIONBEGIN.equals(entry.getEntryType())) {
                            // 当前事务开始位点
                            entryPosition = new EntryPosition(logfilename, logfileoffset, logposTimestamp);
                            logger.debug("set {} to be pending start position before finding another proper one...",
                                entryPosition);
                            logPosition.setPostion(entryPosition);
                        }

                        lastPosition = buildLastPosition(entry);
                    } catch (Exception e) {
                        processError(e, lastPosition, searchBinlogFile, 4L);
                    }

                    return running;
                }
            });

        } catch (IOException e) {
            logger.error("ERROR ## findAsPerTimestampInSpecificLogFile has an error", e);
        }

        if (logPosition.getPostion() != null) {
            return logPosition.getPostion();
        } else {
            return null;
        }
    }

    public void setSupportBinlogFormats(String formatStrs) {
        String[] formats = StringUtils.split(formatStrs, ',');
        if (formats != null) {
            BinlogFormat[] supportBinlogFormats = new BinlogFormat[formats.length];
            int i = 0;
            for (String format : formats) {
                supportBinlogFormats[i++] = BinlogFormat.valuesOf(format);
            }

            this.supportBinlogFormats = supportBinlogFormats;
        }
    }

    public void setSupportBinlogImages(String imageStrs) {
        String[] images = StringUtils.split(imageStrs, ',');
        if (images != null) {
            BinlogImage[] supportBinlogImages = new BinlogImage[images.length];
            int i = 0;
            for (String image : images) {
                supportBinlogImages[i++] = BinlogImage.valuesOf(image);
            }

            this.supportBinlogImages = supportBinlogImages;
        }
    }

    // ===================== setter / getter ========================

    public void setDefaultConnectionTimeoutInSeconds(int defaultConnectionTimeoutInSeconds) {
        this.defaultConnectionTimeoutInSeconds = defaultConnectionTimeoutInSeconds;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public void setMasterInfo(AuthenticationInfo masterInfo) {
        this.masterInfo = masterInfo;
    }

    public void setStandbyInfo(AuthenticationInfo standbyInfo) {
        this.standbyInfo = standbyInfo;
    }

    public void setMasterPosition(EntryPosition masterPosition) {
        this.masterPosition = masterPosition;
    }

    public void setStandbyPosition(EntryPosition standbyPosition) {
        this.standbyPosition = standbyPosition;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public void setDetectingSQL(String detectingSQL) {
        this.detectingSQL = detectingSQL;
    }

    public void setDetectingIntervalInSeconds(Integer detectingIntervalInSeconds) {
        this.detectingIntervalInSeconds = detectingIntervalInSeconds;
    }

    public void setDetectingEnable(boolean detectingEnable) {
        this.detectingEnable = detectingEnable;
    }

    public void setFallbackIntervalInSeconds(int fallbackIntervalInSeconds) {
        this.fallbackIntervalInSeconds = fallbackIntervalInSeconds;
    }

    public CanalHAController getHaController() {
        return haController;
    }

    public void setHaController(CanalHAController haController) {
        this.haController = haController;
    }
}
