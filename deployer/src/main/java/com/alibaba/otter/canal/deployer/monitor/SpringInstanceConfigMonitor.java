package com.alibaba.otter.canal.deployer.monitor;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MigrateMap;

/**
 * 监听基于spring配置的instance变化
 * 
 * @author jianghang 2013-2-6 下午06:23:55
 * @version 1.0.1
 */
public class SpringInstanceConfigMonitor extends AbstractCanalLifeCycle implements InstanceConfigMonitor, CanalLifeCycle {

    private static final Logger              logger               = LoggerFactory.getLogger(SpringInstanceConfigMonitor.class);
    //目录 /rootConf/${destination}/instance.properties
    private String                           rootConf;//目录--该目录下有若干个子目录,每一个destination都在该目录下产生一个新的目录
    // 扫描周期，单位秒
    private long                             scanIntervalInSecond = 5;
    private InstanceAction                   defaultAction        = null;//默认的加载配置文件的类
    private Map<String, InstanceAction>      actions              = new MapMaker().makeMap();//每一个destination,用哪种行为方式去加载配置信息
    private Map<String, InstanceConfigFiles> lastFiles            = MigrateMap.makeComputingMap(new Function<String, InstanceConfigFiles>() {//每一个destination 对应一个文件的详细信息

                                                                      public InstanceConfigFiles apply(String destination) {
                                                                          return new InstanceConfigFiles(destination);
                                                                      }
                                                                  });
    private ScheduledExecutorService         executor             = Executors.newScheduledThreadPool(1,
                                                                      new NamedThreadFactory("canal-instance-scan"));//线程池

    public void start() {
        super.start();
        Assert.notNull(rootConf, "root conf dir is null!");

        executor.scheduleWithFixedDelay(new Runnable() {

            public void run() {
                try {
                    scan();
                } catch (Throwable e) {
                    logger.error("scan failed", e);
                }
            }

        }, 0, scanIntervalInSecond, TimeUnit.SECONDS);
    }

    public void stop() {
        super.stop();
        executor.shutdownNow();
        actions.clear();
        lastFiles.clear();
    }

    public void register(String destination, InstanceAction action) {
        if (action != null) {
            actions.put(destination, action);
        } else {
            actions.put(destination, defaultAction);
        }
    }

    public void unregister(String destination) {
        actions.remove(destination);
    }

    public void setRootConf(String rootConf) {
        this.rootConf = rootConf;
    }

    private void scan() {
        File rootdir = new File(rootConf);
        if (!rootdir.exists()) {
            return;
        }

        //获取destination目录集合
        File[] instanceDirs = rootdir.listFiles(new FileFilter() {

            public boolean accept(File pathname) {//true的是最终要的文件
                String filename = pathname.getName();
                return pathname.isDirectory() && !"spring".equalsIgnoreCase(filename);//要的是目录.并且排除spring这个目录
            }
        });

        // 扫描目录的新增
        Set<String> currentInstanceNames = new HashSet<String>();

        // 判断目录内文件的变化
        for (File instanceDir : instanceDirs) {
            String destination = instanceDir.getName();
            currentInstanceNames.add(destination);
            File[] instanceConfigs = instanceDir.listFiles(new FilenameFilter() {//找到目录下的instance.properties文件

                public boolean accept(File dir, String name) {
                    // return !StringUtils.endsWithIgnoreCase(name, ".dat");
                    // 限制一下，只针对instance.properties文件,避免因为.svn或者其他生成的临时文件导致出现reload
                    return StringUtils.equalsIgnoreCase(name, "instance.properties");
                }

            });

            if (!actions.containsKey(destination) && instanceConfigs.length > 0) {
                // 存在合法的instance.properties，并且第一次添加时，进行启动操作
                notifyStart(instanceDir, destination);
            } else if (actions.containsKey(destination)) {
                // 历史已经启动过
                if (instanceConfigs.length == 0) { // 如果不存在合法的instance.properties
                    notifyStop(destination);
                } else {
                    InstanceConfigFiles lastFile = lastFiles.get(destination);
                    boolean hasChanged = judgeFileChanged(instanceConfigs, lastFile.getInstanceFiles());
                    // 通知变化
                    if (hasChanged) {
                        notifyReload(destination);
                    }

                    if (hasChanged || CollectionUtils.isEmpty(lastFile.getInstanceFiles())) {
                        // 更新内容
                        List<FileInfo> newFileInfo = new ArrayList<FileInfo>();
                        for (File instanceConfig : instanceConfigs) {
                            newFileInfo.add(new FileInfo(instanceConfig.getName(), instanceConfig.lastModified()));
                        }

                        lastFile.setInstanceFiles(newFileInfo);
                    }
                }
            }

        }

        // 判断目录是否删除--说有有一些destination已经不需要监控了
        Set<String> deleteInstanceNames = new HashSet<String>();
        for (String destination : actions.keySet()) {
            if (!currentInstanceNames.contains(destination)) {
                deleteInstanceNames.add(destination);
            }
        }
        for (String deleteInstanceName : deleteInstanceNames) {
            notifyStop(deleteInstanceName);
        }
    }

    private void notifyStart(File instanceDir, String destination) {
        try {
            defaultAction.start(destination);
            actions.put(destination, defaultAction);
            logger.info("auto notify start {} successful.", destination);
        } catch (Throwable e) {
            logger.error("scan add found[{}] but start failed", destination, ExceptionUtils.getFullStackTrace(e));
        }
    }

    private void notifyStop(String destination) {
        InstanceAction action = actions.remove(destination);
        try {
            action.stop(destination);
            logger.info("auto notify stop {} successful.", destination);
        } catch (Throwable e) {
            logger.error("scan delete found[{}] but stop failed", destination, ExceptionUtils.getFullStackTrace(e));
            actions.put(destination, action);// 再重新加回去，下一次scan时再执行删除
        }
    }

    private void notifyReload(String destination) {
        InstanceAction action = actions.get(destination);
        if (action != null) {
            try {
                action.reload(destination);
                logger.info("auto notify reload {} successful.", destination);
            } catch (Throwable e) {
                logger.error("scan reload found[{}] but reload failed",
                    destination,
                    ExceptionUtils.getFullStackTrace(e));
            }
        }
    }

    //判断文件是否有更改
    private boolean judgeFileChanged(File[] instanceConfigs, List<FileInfo> fileInfos) {
        boolean hasChanged = false;
        for (File instanceConfig : instanceConfigs) {//所有的instance.properties
            for (FileInfo fileInfo : fileInfos) {
                if (instanceConfig.getName().equals(fileInfo.getName())) {
                    hasChanged |= (instanceConfig.lastModified() != fileInfo.getLastModified());
                    if (hasChanged) {
                        return hasChanged;
                    }
                }
            }
        }

        return hasChanged;
    }

    public void setDefaultAction(InstanceAction defaultAction) {
        this.defaultAction = defaultAction;
    }

    public void setScanIntervalInSecond(long scanIntervalInSecond) {
        this.scanIntervalInSecond = scanIntervalInSecond;
    }

    public static class InstanceConfigFiles {

        private String         destination;                              // instance
                                                                          // name
        private List<FileInfo> springFile    = new ArrayList<FileInfo>(); // spring的instance
                                                                          // xml
        private FileInfo       rootFile;                                 // canal.properties
        private List<FileInfo> instanceFiles = new ArrayList<FileInfo>(); // instance对应的配置

        public InstanceConfigFiles(String destination){
            this.destination = destination;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public List<FileInfo> getSpringFile() {
            return springFile;
        }

        public void setSpringFile(List<FileInfo> springFile) {
            this.springFile = springFile;
        }

        public FileInfo getRootFile() {
            return rootFile;
        }

        public void setRootFile(FileInfo rootFile) {
            this.rootFile = rootFile;
        }

        public List<FileInfo> getInstanceFiles() {
            return instanceFiles;
        }

        public void setInstanceFiles(List<FileInfo> instanceFiles) {
            this.instanceFiles = instanceFiles;
        }

    }

    //记录每一个文件的最后修改时间
    public static class FileInfo {

        private String name;
        private long   lastModified = 0;

        public FileInfo(String name, long lastModified){
            this.name = name;
            this.lastModified = lastModified;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getLastModified() {
            return lastModified;
        }

        public void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

    }

}
