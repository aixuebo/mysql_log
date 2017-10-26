package com.alibaba.otter.canal.store;

/**
 * @author zebin.xuzb 2012-10-30 下午1:05:13
 * @since 1.0.0
 * 代表一个存储器
 */
public class StoreInfo {

    private String storeName;//存储器的name
    private String filter;

    public String getStoreName() {
        return storeName;
    }

    public String getFilter() {
        return filter;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

}
