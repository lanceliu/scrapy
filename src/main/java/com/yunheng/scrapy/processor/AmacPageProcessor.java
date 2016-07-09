package com.yunheng.scrapy.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yunheng.scrapy.downloader.JSONRequestDownloader;
import com.yunheng.scrapy.pipeline.OneExcelPipeline;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.utils.HttpConstant;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

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
    public static final int PAGE_SIZE = 2;
    public static final String URL_START_URL = "http://gs.amac.org.cn/amac-infodisc/api/pof/manager?page=%s&size=%s";
    public static final String URL_LIST = "http://gs\\.amac\\.org\\.cn/amac-infodisc/api/pof/manager\\?page=\\d+&size=\\d+";
    public static final String URL_DETAIL = "http://gs.amac.org.cn/amac-infodisc/res/pof/manager/%s";

    public void process(Page page) {

        if (page.getUrl().regex(URL_LIST).match()) {

            page.getJson().jsonPath("$.content[*]").nodes().forEach( node->{
                Request template = new Request();
                JSONObject json = JSON.parseObject( node.get() );
                template.setUrl(String.format(URL_DETAIL, json.getString("url")));
                template.putExtra("managerName", json.getString("managerName")); // 私募基金管理人名称
                template.putExtra("artificialPersonName", json.getString("artificialPersonName")); // 法定代表人/执行事务合伙人
                template.putExtra("city", json.getString("regAdrAgg"));// 注册地
                template.putExtra("registerNo", json.getString("registerNo"));// 登记编号
                template.putExtra("primaryInvestType", json.getString("primaryInvestType")); // 基金主要类别
                template.putExtra("establishDate", DateFormatUtils.format(json.getDate("establishDate"), "yyyy-MM-dd"));// 成立时间
                template.putExtra("registerDate", DateFormatUtils.format(json.getDate("registerDate"), "yyyy-MM-dd"));// 登记时间
                page.addTargetRequest( template );
            });

            try {
                if ( !Boolean.parseBoolean(  page.getJson().jsonPath("$.last").get()  ) ) {
                    int pageNum = Integer.parseInt(  page.getJson().jsonPath("$.number").get() ) + 1;
                    page.addTargetRequest( getListRequest(pageNum) );
                }
            } catch ( Exception e ) {
                logger.warn("", e);
            }
        } else {
            page.putField("managerName", page.getRequest().getExtra("managerName"));
            page.putField("artificialPersonName", page.getRequest().getExtra("artificialPersonName"));
            page.putField("city", page.getRequest().getExtra("city"));
            page.putField("registerNo", page.getRequest().getExtra("registerNo"));
            page.putField("primaryInvestType", page.getRequest().getExtra("primaryInvestType"));
            page.putField("establishDate", page.getRequest().getExtra("establishDate"));
            page.putField("registerDate", page.getRequest().getExtra("registerDate"));

            page.putField("applyBizType", page.getHtml().xpath("/html/body/div[1]/div[2]/div/table/tbody/tr[12]/td[4]/text()")); // 申请的其他业务类型
            page.putField("registerAddress", page.getHtml().xpath("/html/body/div[1]/div[2]/div/table/tbody/tr[8]/td[2]/text()")); // 注册地址
            page.putField("officeAddress", page.getHtml().xpath("/html/body/div[1]/div[2]/div/table/tbody/tr[9]/td[2]/text()")); // 办公地址
            page.putField("orgSiteUrl", page.getHtml().xpath("/html/body/div[1]/div[2]/div/table/tbody/tr[13]/td[4]/a/text()")); // 机构网址

            StringBuilder recordsFunds = new StringBuilder();
            page.getHtml().xpath("/html/body/div[1]/div[2]/div/table/tbody/tr[22]/td[2]/p/a/text()")
                    .all().forEach( str -> {
                recordsFunds.append( str ).append(System.getProperties().getProperty("line.separator"));
            });
            page.putField("recordedFund", recordsFunds );  // 备案基金
        }
    }

    public Site getSite() {
        return site;
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        List<String> excelCaptionList = new ArrayList<String>() {
            {
                add("编号");
                add("私募基金管理人名称");
                add("法定代表人/执行事务合伙人(委派代表)姓名");
                add("管理基金主要类别");
                add("申请的其他业务类型");
                add("注册地");
                add("登记编号");
                add("成立时间");
                add("登记时间");
                add("注册地址");
                add("办公地址");
                add("机构网址");
                add("备案基金");
            }
        };
        Spider.create(new AmacPageProcessor()).setDownloader( new JSONRequestDownloader() ).thread(3).addRequest( getListRequest(1) )
                .addPipeline( new OneExcelPipeline("data/webmagic/mamacn/data.xls", excelCaptionList, "私募管理人列表") ).run();
    }

    private static Request getListRequest( int pageNum  ) {
        if ( pageNum < 1 ) pageNum = 1;
        Request request = new Request(String.format(URL_START_URL, pageNum, PAGE_SIZE));
        request.setMethod( HttpConstant.Method.POST );
        request.putExtra("jsonData", "{}");
        return request;
    }
}
