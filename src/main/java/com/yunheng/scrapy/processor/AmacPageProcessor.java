package com.yunheng.scrapy.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yunheng.scrapy.downloader.JSONRequestDownloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.JsonPathSelector;
import us.codecraft.webmagic.utils.HttpConstant;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 中国证券投资基金业协会:私募基金管理人
 *
 * @author lanceliu <liuyunfei@yuntujinfu.com>
 * @date 16/7/6
 */
public class AmacPageProcessor implements PageProcessor {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private Site site = Site.me().setDomain("gs.amac.org.cn")
            .addHeader("Content-Type", "application/json");
    public static final int PAGE_SIZE = 20;
    public static final String URL_START_URL = "http://gs.amac.org.cn/amac-infodisc/api/pof/manager?page=%s&size=%s";
    public static final String URL_LIST = "http://gs\\.amac\\.org\\.cn/amac-infodisc/api/pof/manager\\?page=\\d+&size=\\d+";
    public static final String URL_DETAIL = "http://gs.amac.org.cn/amac-infodisc/res/pof/manager/%s";

    public void process(Page page) {

        if (page.getUrl().regex(URL_LIST).match()) {

            JsonPathSelector detailSelector = new JsonPathSelector("$.content[*].url");
//            List<String> childDetails = (List<String>) detailSelector.selectList(  page.getRawText() ).stream().map(l-> {
//                return String.format(URL_DETAIL, l);
//            }).collect(Collectors.toCollection(ArrayList<String>::new));
//            page.addTargetRequests( childDetails );
//            page.addTargetRequest(childDetails.get(0));

            List<Request> childDetails = page.getJson().jsonPath("$.content[*]").nodes().stream().map( node->{
                Request template = new Request();
                JSONObject json = JSON.parseObject( node.get() );
                template.setUrl(String.format(URL_DETAIL, json.getString("url")));
                template.putExtra("managerName", json.getString("managerName")); // 私募基金管理人名称
                template.putExtra("artificialPersonName", json.getString("artificialPersonName")); // 法定代表人/执行事务合伙人
                template.putExtra("city", json.getString("regAdrAgg"));// 注册地
                template.putExtra("registerNo", json.getString("registerNo"));// 登记编号
                template.putExtra("primaryInvestType", json.getString("primaryInvestType")); // 基金主要类别
                template.putExtra("establishDate", json.getDate("establishDate"));// 成立时间
                template.putExtra("registerDate", json.getDate("registerDate"));// 登记时间
                return template;
            }).collect(Collectors.toCollection(ArrayList<Request>::new));

            page.addTargetRequest(childDetails.get(0));
//            try {
//                if ( Boolean.parseBoolean(  page.getJson().jsonPath("$.last").get()  ) ) {
//                    int pageNum = Integer.parseInt(  page.getJson().jsonPath("$.number").get() ) + 1;
//                    page.addTargetRequest( getListRequest(pageNum, PAGE_SIZE) );
//                }
//            } catch ( Exception e ) {
//                logger.warn("", e);
//            }
        } else {
            page.putField("managerName", page.getRequest().getExtra("managerName"));
            page.putField("artificialPersonName", page.getRequest().getExtra("artificialPersonName"));
            page.putField("city", page.getRequest().getExtra("city"));
            page.putField("registerNo", page.getRequest().getExtra("registerNo"));
            page.putField("primaryInvestType", page.getRequest().getExtra("primaryInvestType"));
            page.putField("establishDate", page.getRequest().getExtra("establishDate"));
            page.putField("registerDate", page.getRequest().getExtra("registerDate"));

        }
    }

    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new AmacPageProcessor()).setDownloader( new JSONRequestDownloader() ).addRequest( getListRequest(1, 20) ).run();
    }

    private static Request getListRequest( int pageNum, int pageSize ) {
        if ( pageSize <1 ) pageSize = PAGE_SIZE;
        if ( pageNum < 1 ) pageNum = 1;
        Request request = new Request(String.format(URL_START_URL, pageNum, pageSize));
        request.setMethod( HttpConstant.Method.POST );
        request.putExtra("jsonData", "{}");
        return request;
    }
}
