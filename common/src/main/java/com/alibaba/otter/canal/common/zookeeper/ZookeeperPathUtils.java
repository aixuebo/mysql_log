package com.alibaba.otter.canal.common.zookeeper;

import java.text.MessageFormat;

import org.apache.commons.lang.StringUtils;

/**
 * 存储结构：
 * 
 * <pre>
 * /otter
 *    canal
 *      cluster  存储有哪些集群节点,即ip:port
 *      destinations
 *        dest1 每一个destination目的地
 *          running (EPHEMERAL)  存储ServerRunningData对象
 *          cluster
 *          parse---存储LogPosition信息
 *          client1 每一个客户端--存储客户端ID
 *            running (EPHEMERAL)
 *            cluster
 *            filter 记录filter过滤信息--存储字符串
 *            cursor 游标位置信息 获取该客户端下的cursor游标信息,Position的json形式存储
 *            mark 记录批处理信息
 *              1 每一个批处理信息  存储PositionRange
 *              2
 *              3
 * </pre>
 * 
 * @author zebin.xuzb @ 2012-6-21
 * @version 1.0.0
 */
public class ZookeeperPathUtils {

    public static final String ZOOKEEPER_SEPARATOR                          = "/";

    public static final String OTTER_ROOT_NODE                              = ZOOKEEPER_SEPARATOR + "otter";// /otter

    public static final String CANAL_ROOT_NODE                              = OTTER_ROOT_NODE + ZOOKEEPER_SEPARATOR // /otter/canal
                                                                              + "canal";

    public static final String DESTINATION_ROOT_NODE                        = CANAL_ROOT_NODE + ZOOKEEPER_SEPARATOR  // /otter/canal/destinations
                                                                              + "destinations";

    public static final String FILTER_NODE                                  = "filter";

    public static final String BATCH_MARK_NODE                              = "mark";

    public static final String PARSE_NODE                                   = "parse";

    public static final String CURSOR_NODE                                  = "cursor";

    public static final String RUNNING_NODE                                 = "running";

    public static final String CLUSTER_NODE                                 = "cluster";

    public static final String DESTINATION_NODE                             = DESTINATION_ROOT_NODE
                                                                              + ZOOKEEPER_SEPARATOR + "{0}";// /otter/canal/destinations/{0}

    public static final String DESTINATION_PARSE_NODE                       = DESTINATION_NODE + ZOOKEEPER_SEPARATOR
                                                                              + PARSE_NODE;// /otter/canal/destinations/{0}/parse   存储LogPosition信息

    public static final String DESTINATION_CLIENTID_NODE                    = DESTINATION_NODE + ZOOKEEPER_SEPARATOR
                                                                              + "{1}";// /otter/canal/destinations/{0}/{1} //0表示destination  1表示clientId

    public static final String DESTINATION_CURSOR_NODE                      = DESTINATION_CLIENTID_NODE
                                                                              + ZOOKEEPER_SEPARATOR + CURSOR_NODE;// /otter/canal/destinations/{0}/{1}/cursor  获取该客户端下的cursor游标信息,Position的json形式存储

    public static final String DESTINATION_CLIENTID_FILTER_NODE             = DESTINATION_CLIENTID_NODE
                                                                              + ZOOKEEPER_SEPARATOR + FILTER_NODE;// /otter/canal/destinations/{0}/{1}/filter   将filter的内容写入到该path下,字符串形式存储

    public static final String DESTINATION_CLIENTID_BATCH_MARK_NODE         = DESTINATION_CLIENTID_NODE
                                                                              + ZOOKEEPER_SEPARATOR + BATCH_MARK_NODE;// /otter/canal/destinations/{0}/{1}/mark  存储batch信息的根目录,具体会跟一个顺序序号的子目录ID

    public static final String DESTINATION_CLIENTID_BATCH_MARK_WITH_ID_PATH = DESTINATION_CLIENTID_BATCH_MARK_NODE
                                                                              + ZOOKEEPER_SEPARATOR + "{2}"; // /otter/canal/destinations/{0}/{1}/mark/{2} //batch的子目录,表示一个批处理,存储的内容是PositionRange对象的json形式

    /**
     * 服务端当前正在提供服务的running节点
     */
    public static final String DESTINATION_RUNNING_NODE                     = DESTINATION_NODE + ZOOKEEPER_SEPARATOR
                                                                              + RUNNING_NODE;//  /otter/canal/destinations/{0}/running   存储ServerRunningData对象

    /**
     * 客户端当前正在工作的running节点
     */
    public static final String DESTINATION_CLIENTID_RUNNING_NODE            = DESTINATION_CLIENTID_NODE
                                                                              + ZOOKEEPER_SEPARATOR + RUNNING_NODE; // /otter/canal/destinations/{0}/{1}/running

    /**
     * 整个canal server的集群列表
     */
    public static final String CANAL_CLUSTER_ROOT_NODE                      = CANAL_ROOT_NODE + ZOOKEEPER_SEPARATOR
                                                                              + CLUSTER_NODE;// /otter/canal/cluster

    //存储ip:port信息,表示集群中的一个节点
    public static final String CANAL_CLUSTER_NODE                           = CANAL_CLUSTER_ROOT_NODE
                                                                              + ZOOKEEPER_SEPARATOR + "{0}"; // /otter/canal/cluster/{0}  表示集群中的一个节点

    /**
     * 针对某个destination的工作的集群列表
     */
    public static final String DESTINATION_CLUSTER_ROOT                     = DESTINATION_NODE + ZOOKEEPER_SEPARATOR
                                                                              + CLUSTER_NODE;// /otter/canal/destinations/{0}
    public static final String DESTINATION_CLUSTER_NODE                     = DESTINATION_CLUSTER_ROOT
                                                                              + ZOOKEEPER_SEPARATOR + "{1}";//  /otter/canal/destinations/{0}/{1}

    public static String getDestinationPath(String destinationName) {
        return MessageFormat.format(DESTINATION_NODE, destinationName);
    }

    public static String getClientIdNodePath(String destinationName, short clientId) {
        return MessageFormat.format(DESTINATION_CLIENTID_NODE, destinationName, String.valueOf(clientId));
    }

    public static String getFilterPath(String destinationName, short clientId) {
        return MessageFormat.format(DESTINATION_CLIENTID_FILTER_NODE, destinationName, String.valueOf(clientId));
    }

    public static String getBatchMarkPath(String destinationName, short clientId) {
        return MessageFormat.format(DESTINATION_CLIENTID_BATCH_MARK_NODE, destinationName, String.valueOf(clientId));
    }

    public static String getBatchMarkWithIdPath(String destinationName, short clientId, Long batchId) {
        return MessageFormat.format(DESTINATION_CLIENTID_BATCH_MARK_WITH_ID_PATH,
            destinationName,
            String.valueOf(clientId),
            getBatchMarkNode(batchId));
    }

    public static String getCursorPath(String destination, short clientId) {
        return MessageFormat.format(DESTINATION_CURSOR_NODE, destination, String.valueOf(clientId));
    }

    public static String getCanalClusterNode(String node) {
        return MessageFormat.format(CANAL_CLUSTER_NODE, node);
    }

    /**
     * 服务端当前正在提供服务的running节点
     */
    public static String getDestinationServerRunning(String destination) {
        return MessageFormat.format(DESTINATION_RUNNING_NODE, destination);
    }

    /**
     * 客户端当前正在工作的running节点
     */
    public static String getDestinationClientRunning(String destination, short clientId) {
        return MessageFormat.format(DESTINATION_CLIENTID_RUNNING_NODE, destination, String.valueOf(clientId));
    }

    public static String getDestinationClusterNode(String destination, String node) {
        return MessageFormat.format(DESTINATION_CLUSTER_NODE, destination, node);
    }

    public static String getDestinationClusterRoot(String destination) {
        return MessageFormat.format(DESTINATION_CLUSTER_ROOT, destination);
    }

    public static String getParsePath(String destination) {
        return MessageFormat.format(DESTINATION_PARSE_NODE, destination);
    }

    /**
     * 将batchNode转换为Long
     */
    public static short getClientId(String clientNode) {
        return Short.valueOf(clientNode);
    }

    /**
     * 将batchNode转换为Long
     */
    public static long getBatchMarkId(String batchMarkNode) {
        return Long.valueOf(batchMarkNode);
    }

    /**
     * 将batchId转化为zookeeper中的node名称
     */
    public static String getBatchMarkNode(Long batchId) {
        return StringUtils.leftPad(String.valueOf(batchId.intValue()), 10, '0');
    }
}
