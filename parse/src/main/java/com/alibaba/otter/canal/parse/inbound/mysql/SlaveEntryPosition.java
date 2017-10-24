package com.alibaba.otter.canal.parse.inbound.mysql;

import com.alibaba.otter.canal.protocol.position.EntryPosition;

/**
 * slave status状态的信息
 * 通过下面四个属性,可以知道该slave读取到master哪个binlog文件,哪个偏移量位置了,以及master的host和port是什么
 * @author jianghang 2013-1-23 下午09:42:18
 * @version 1.0.0
 */
public class SlaveEntryPosition extends EntryPosition {

    private static final long serialVersionUID = 5271424551446372093L;
    private final String      masterHost;
    private final String      masterPort;

    public SlaveEntryPosition(String fileName, long position, String masterHost, String masterPort){
        super(fileName, position);

        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public String getMasterPort() {
        return masterPort;
    }
}
