package com.alibaba.otter.canal.parse.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author zebin.xuzb 2012-9-26 下午3:08:11
 * @version 1.0.0
 */
public class HaAuthenticationInfo {

    private AuthenticationInfo       master;//连接数据库的主库信息
    private List<AuthenticationInfo> slavers = new ArrayList<AuthenticationInfo>();//从库信息集合

    public AuthenticationInfo getMaster() {
        return master;
    }

    public void setMaster(AuthenticationInfo master) {
        this.master = master;
    }

    public List<AuthenticationInfo> getSlavers() {
        return slavers;
    }

    public void addSlaver(AuthenticationInfo slaver) {
        this.slavers.add(slaver);
    }

    public void addSlavers(Collection<AuthenticationInfo> slavers) {
        this.slavers.addAll(slavers);
    }

}
