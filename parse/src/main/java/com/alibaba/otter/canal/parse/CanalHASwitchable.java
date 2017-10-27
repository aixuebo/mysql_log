package com.alibaba.otter.canal.parse;

import com.alibaba.otter.canal.parse.support.AuthenticationInfo;

/**
 * 支持可切换的数据复制控制器----切换的是mysql的服务,即如果master挂了/或者压力大了,我可以切换到他的从库上去继续读数据
 * 
 * @author jianghang 2012-6-26 下午05:41:43
 * @version 1.0.0
 */
public interface CanalHASwitchable {

    //如何切换服务器,在这里面写,包含怎么保存现在服务器的内容等信息
    public void doSwitch();

    //切换到哪个mysql服务上去解析binlog
    public void doSwitch(AuthenticationInfo newAuthenticationInfo);
}
