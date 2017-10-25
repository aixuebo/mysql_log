package com.alibaba.otter.canal.common.zookeeper.running;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * 服务端running状态信息
 * 
 * @author jianghang 2012-11-22 下午03:11:30
 * @version 1.0.0
 */
public class ServerRunningData implements Serializable {

    private static final long serialVersionUID = 92260481691855281L;

    private Long              cid;//节点的序号,每一个节点序号唯一
    private String            address;//节点的ip
    private boolean           active           = true;//true表示此时该节点是活跃的,集群中只有一个节点是活跃的

    public ServerRunningData(){
    }

    public ServerRunningData(Long cid, String address){
        this.cid = cid;
        this.address = address;
    }

    public Long getCid() {
        return cid;
    }

    public void setCid(Long cid) {
        this.cid = cid;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
