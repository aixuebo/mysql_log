package com.alibaba.otter.canal.protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

/**
 * @author zebin.xuzb @ 2012-6-19
 * @version 1.0.0
 * 客户端一次性抓去多个事件回来,因此一次性抓去对于客户端来说就是一个批处理,即该类表示一个批处理
 */
public class Message implements Serializable {

    private static final long      serialVersionUID = 1234034768477580009L;

    private long                   id;//表示一个批处理的ID
    private List<CanalEntry.Entry> entries          = new ArrayList<CanalEntry.Entry>();//抓去回来的事件集合

    public Message(long id, List<Entry> entries){
        this.id = id;
        this.entries = entries == null ? new ArrayList<Entry>() : entries;
    }

    public Message(long id){
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public void setEntries(List<CanalEntry.Entry> entries) {
        this.entries = entries;
    }

    public void addEntry(CanalEntry.Entry entry) {
        this.entries.add(entry);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
