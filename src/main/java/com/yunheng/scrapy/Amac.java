package com.yunheng.scrapy;

import us.codecraft.webmagic.model.HasKey;
import us.codecraft.webmagic.model.annotation.HelpUrl;
import us.codecraft.webmagic.model.annotation.TargetUrl;

/**
 *
 * @author lanceliu <liuyunfei@yuntujinfu.com>
 * @date 16/7/6
 */
@TargetUrl("http://gs.amac.org.cn/amac-infodisc/res/pof/manager/\\w+")
@HelpUrl("http://gs.amac.org.cn/amac-infodisc/api/pof/manager\\w+")
public class Amac implements HasKey{
    public String key() {
        return null;
    }
}
