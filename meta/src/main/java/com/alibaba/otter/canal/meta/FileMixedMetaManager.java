package com.alibaba.otter.canal.meta;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.MigrateMap;

/**
 * 基于文件刷新的metaManager实现----基于内存方式的一种扩充,即定期将内存的信息保存到文件中
 * 
 * <pre>
 * 策略：
 * 1. 先写内存，然后定时刷新数据到File
 * 2. 数据采取overwrite模式(只保留最后一次)，通过logger实施append模式(记录历史版本)
 * </pre>
 * 
 * @author jianghang 2013-4-15 下午05:55:57
 * @version 1.0.4
 */
public class FileMixedMetaManager extends MemoryMetaManager implements CanalMetaManager {

    private static final Logger      logger       = LoggerFactory.getLogger(FileMixedMetaManager.class);
    private static final Charset     charset      = Charset.forName("UTF-8");
    private File                     dataDir;//文件所在的目录
    private String                   dataFileName = "meta.dat";//文件名字   dataDir/destination/meta.dat
    private Map<String, File>        dataFileCaches;//每一个destination 对应一个文件,即key是destination,value是File
    private ScheduledExecutorService executor;//线程池对象
    @SuppressWarnings("serial")
    private final Position           nullCursor   = new Position() {
                                                  };
    private long                     period       = 1000;                                               // 单位ms 每个周期执行线程池
    private Set<ClientIdentity>      updateCursorTasks;//在周期内有变化的客户端,定期会对这些客户端进行save保存

    public void start() {
        super.start();
        Assert.notNull(dataDir);
        if (!dataDir.exists()) {//创建目录
            try {
                FileUtils.forceMkdir(dataDir);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }

        if (!dataDir.canRead() || !dataDir.canWrite()) {
            throw new CanalMetaManagerException("dir[" + dataDir.getPath() + "] can not read/write");
        }

        dataFileCaches = MigrateMap.makeComputingMap(new Function<String, File>() {

            public File apply(String destination) {
                return getDataFile(destination);
            }
        });

        executor = Executors.newScheduledThreadPool(1);

        //加载destination的文件信息
        destinations = MigrateMap.makeComputingMap(new Function<String, List<ClientIdentity>>() {

            public List<ClientIdentity> apply(String destination) {
                return loadClientIdentity(destination);
            }
        });

        cursors = MigrateMap.makeComputingMap(new Function<ClientIdentity, Position>() {

            public Position apply(ClientIdentity clientIdentity) {
                Position position = loadCursor(clientIdentity.getDestination(), clientIdentity);
                if (position == null) {
                    return nullCursor; // 返回一个空对象标识，避免出现异常
                } else {
                    return position;
                }
            }
        });

        updateCursorTasks = Collections.synchronizedSet(new HashSet<ClientIdentity>());

        // 启动定时工作任务
        executor.scheduleAtFixedRate(new Runnable() {

            public void run() {
                List<ClientIdentity> tasks = new ArrayList<ClientIdentity>(updateCursorTasks);
                for (ClientIdentity clientIdentity : tasks) {
                    MDC.put("destination", String.valueOf(clientIdentity.getDestination()));
                    try {
                        // 定时将内存中的最新值刷到file中，多次变更只刷一次
                        if (logger.isInfoEnabled()) {
                            LogPosition cursor = (LogPosition) getCursor(clientIdentity);
                            logger.info("clientId:{} cursor:[{},{},{}] address[{}]",
                                new Object[] { clientIdentity.getClientId(), cursor.getPostion().getJournalName(),
                                        cursor.getPostion().getPosition(), cursor.getPostion().getTimestamp(),
                                        cursor.getIdentity().getSourceAddress().toString() });
                        }
                        flushDataToFile(clientIdentity.getDestination());
                        updateCursorTasks.remove(clientIdentity);
                    } catch (Throwable e) {
                        // ignore
                        logger.error("period update" + clientIdentity.toString() + " curosr failed!", e);
                    }
                }
            }
        },
            period,
            period,
            TimeUnit.MILLISECONDS);
    }

    public void stop() {
        super.stop();

        flushDataToFile();// 刷新数据
        executor.shutdownNow();
        destinations.clear();
        batches.clear();
    }

    public void subscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.subscribe(clientIdentity);

        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(new Runnable() {

            public void run() {
                flushDataToFile(clientIdentity.getDestination());
            }
        });
    }

    public void unsubscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.unsubscribe(clientIdentity);

        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(new Runnable() {

            public void run() {
                flushDataToFile(clientIdentity.getDestination());
            }
        });
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        updateCursorTasks.add(clientIdentity);// 添加到任务队列中进行触发
        super.updateCursor(clientIdentity, position);
    }

    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Position position = super.getCursor(clientIdentity);
        if (position == nullCursor) {
            return null;
        } else {
            return position;
        }
    }

    // ============================ helper method ======================

    //获取文件对象
    private File getDataFile(String destination) {
        File destinationMetaDir = new File(dataDir, destination);
        if (!destinationMetaDir.exists()) {
            try {
                FileUtils.forceMkdir(destinationMetaDir);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }

        return new File(destinationMetaDir, dataFileName);
    }

    //加载一个文件
    private FileMetaInstanceData loadDataFromFile(File dataFile) {
        try {
            if (!dataFile.exists()) {
                return null;
            }

            String json = FileUtils.readFileToString(dataFile, charset.name());
            return JsonUtils.unmarshalFromString(json, FileMetaInstanceData.class);
        } catch (IOException e) {
            throw new CanalMetaManagerException(e);
        }
    }

    //刷新文件
    private void flushDataToFile() {
        for (String destination : destinations.keySet()) {
            flushDataToFile(destination);
        }
    }

    private void flushDataToFile(String destination) {
        flushDataToFile(destination, dataFileCaches.get(destination));
    }

    //刷新文件
    private void flushDataToFile(String destination, File dataFile) {
        FileMetaInstanceData data = new FileMetaInstanceData();
        if (destinations.containsKey(destination)) {
            synchronized (destination.intern()) { // 基于destination控制一下并发更新
                data.setDestination(destination);

                List<FileMetaClientIdentityData> clientDatas = Lists.newArrayList();
                List<ClientIdentity> clientIdentitys = destinations.get(destination);//该destination下所有的客户端集合
                for (ClientIdentity clientIdentity : clientIdentitys) {
                    FileMetaClientIdentityData clientData = new FileMetaClientIdentityData();
                    clientData.setClientIdentity(clientIdentity);
                    Position position = cursors.get(clientIdentity);
                    if (position != null && position != nullCursor) {
                        clientData.setCursor((LogPosition) position);
                    }

                    clientDatas.add(clientData);
                }

                data.setClientDatas(clientDatas);
            }

            //对象转换成json写入到文件中
            String json = JsonUtils.marshalToString(data);
            try {
                FileUtils.writeStringToFile(dataFile, json);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }
    }

    //加载一个destination文件
    private List<ClientIdentity> loadClientIdentity(String destination) {
        List<ClientIdentity> result = Lists.newArrayList();

        FileMetaInstanceData data = loadDataFromFile(dataFileCaches.get(destination));
        if (data == null) {
            return result;
        }

        List<FileMetaClientIdentityData> clientDatas = data.getClientDatas();
        if (clientDatas == null) {
            return result;
        }

        for (FileMetaClientIdentityData clientData : clientDatas) {
            if (clientData.getClientIdentity().getDestination().equals(destination)) {
                result.add(clientData.getClientIdentity());
            }
        }

        return result;
    }

    private Position loadCursor(String destination, ClientIdentity clientIdentity) {
        FileMetaInstanceData data = loadDataFromFile(dataFileCaches.get(destination));
        if (data == null) {
            return null;
        }

        List<FileMetaClientIdentityData> clientDatas = data.getClientDatas();
        if (clientDatas == null) {
            return null;
        }

        for (FileMetaClientIdentityData clientData : clientDatas) {
            if (clientData.getClientIdentity() != null && clientData.getClientIdentity().equals(clientIdentity)) {
                return clientData.getCursor();
            }
        }

        return null;
    }

    /**
     * 描述一个clientIdentity对应的数据对象
     * 
     * @author jianghang 2013-4-15 下午06:19:40
     * @version 1.0.4
     * 每一个客户端一个该对象
     */
    public static class FileMetaClientIdentityData {

        private ClientIdentity clientIdentity;//客户端对象
        private LogPosition    cursor;//该客户端锁对应的游标对象

        public FileMetaClientIdentityData(){

        }

        public FileMetaClientIdentityData(ClientIdentity clientIdentity, MemoryClientIdentityBatch batch,
                                          LogPosition cursor){
            this.clientIdentity = clientIdentity;
            this.cursor = cursor;
        }

        public ClientIdentity getClientIdentity() {
            return clientIdentity;
        }

        public void setClientIdentity(ClientIdentity clientIdentity) {
            this.clientIdentity = clientIdentity;
        }

        public Position getCursor() {
            return cursor;
        }

        public void setCursor(LogPosition cursor) {
            this.cursor = cursor;
        }

    }

    /**
     * 描述整个canal instance对应数据对象
     * 
     * @author jianghang 2013-4-15 下午06:20:22
     * @version 1.0.4
     * 每一个目的地destination对应一个该对象
     */
    public static class FileMetaInstanceData {

        private String                           destination;//属于哪个目的地
        private List<FileMetaClientIdentityData> clientDatas;//该destination下所有的客户端集合

        public FileMetaInstanceData(){

        }

        public FileMetaInstanceData(String destination, List<FileMetaClientIdentityData> clientDatas){
            this.destination = destination;
            this.clientDatas = clientDatas;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public List<FileMetaClientIdentityData> getClientDatas() {
            return clientDatas;
        }

        public void setClientDatas(List<FileMetaClientIdentityData> clientDatas) {
            this.clientDatas = clientDatas;
        }

    }

    public void setDataDir(String dataDir) {
        this.dataDir = new File(dataDir);
    }

    public void setDataDir(File dataDir) {
        this.dataDir = dataDir;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

}
