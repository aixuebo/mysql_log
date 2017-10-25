package com.alibaba.otter.canal.common.zookeeper.running;

/**
 * 触发一下mainstem发生切换
 * 
 * @author jianghang 2012-9-11 下午02:26:03
 * @version 1.0.0
 */
public interface ServerRunningListener {

    /**
     * 启动时,即start的时候,当start调用完成后,调用该函数回调做点事情
     */
    public void processStart();

    /**
     * 关闭时,即stop的时候,当stop调用完成后,调用该函数回调做点事情
     */
    public void processStop();

    /**
     * 触发现在轮到自己做为active，需要载入上一个active的上下文数据
     */
    public void processActiveEnter();

    /**
     * 触发一下当前active模式失败
     */
    public void processActiveExit();

}
