package com.alibaba.otter.canal.filter;

import com.alibaba.otter.canal.filter.exception.CanalFilterException;

/**
 * 数据过滤机制
 * 
 * @author jianghang 2012-7-20 下午03:51:27
 */
public interface CanalEventFilter<T> {

	//对一个对象进行过滤,返回boolean类型   true表示要该事件
    boolean filter(T event) throws CanalFilterException;
}
