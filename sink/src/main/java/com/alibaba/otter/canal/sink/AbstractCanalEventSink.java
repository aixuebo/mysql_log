package com.alibaba.otter.canal.sink;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.filter.CanalEventFilter;

/**
 * @author jianghang 2012-7-23 下午01:02:45
 * 代表一个sink事件的抽象实现类
 */
public abstract class AbstractCanalEventSink<T> extends AbstractCanalLifeCycle implements CanalEventSink<T> {

	//事件过滤器
    protected CanalEventFilter                  filter;
    
    //拦截器集合,当处理事件的时候,要以此调用该集合----拦截器是有顺序的
    protected List<CanalEventDownStreamHandler> handlers = new ArrayList<CanalEventDownStreamHandler>();

    public void setFilter(CanalEventFilter filter) {
        this.filter = filter;
    }

    //添加拦截器
    public void addHandler(CanalEventDownStreamHandler handler) {
        this.handlers.add(handler);
    }

    //获取第N个事件拦截器
    public CanalEventDownStreamHandler getHandler(int index) {
        return this.handlers.get(index);
    }

    public void addHandler(CanalEventDownStreamHandler handler, int index) {
        this.handlers.add(index, handler);
    }

    public void removeHandler(int index) {
        this.handlers.remove(index);
    }

    public void removeHandler(CanalEventDownStreamHandler handler) {
        this.handlers.remove(handler);
    }

    public CanalEventFilter getFilter() {
        return filter;
    }

    public List<CanalEventDownStreamHandler> getHandlers() {
        return handlers;
    }

    public void interrupt() {
        // do nothing
    }

}
