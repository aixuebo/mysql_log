package com.alibaba.otter.canal.instance.core;

/**
 * @author zebin.xuzb @ 2012-7-12
 * @version 1.0.0
 * 如何产生一个Canal实例对象,相当于工厂
 */
public interface CanalInstanceGenerator {

    /**
     * 通过 destination 产生特定的 {@link CanalInstance}
     * 
     * @param destination
     * @return
     * 如何通过name产生实例
     */
    CanalInstance generate(String destination);
}
