package com.yunheng.scrapy.processor;

import com.yunheng.scrapy.downloader.JSONRequestDownloader;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.JsonPathSelector;
import us.codecraft.webmagic.utils.HttpConstant;

import java.util.List;

/**
 * 中国证券投资基金业协会:私募基金管理人
 *
 * @author lanceliu <liuyunfei@yuntujinfu.com>
 * @date 16/7/6
 */
public class AmacPageProcessor implements PageProcessor {

    private Site site = Site.me().setDomain("gs.amac.org.cn")
            .addHeader("Content-Type", "application/json");
    public static final int PAGE_SIZE = 20;
    public static final String URL_START_URL = "http://gs.amac.org.cn/amac-infodisc/api/pof/manager?page=%s&size=%s";
    public static final String URL_LIST = "http://gs\\.amac\\.org\\.cn/amac-infodisc/api/pof/manager\\?page=\\d+&size=\\d+";
    public static final String URL_DETAIL = "http://gs.amac.org.cn/amac-infodisc/res/pof/manager/%s";

    public void process(Page page) {

        if (page.getUrl().regex(URL_LIST).match()) {
            JsonPathSelector jsonPathSelector = new JsonPathSelector("$.content[*].url");

            List<String> childDetails = (List<String>) jsonPathSelector.selectList( page.getRawText() );
            page.addTargetRequests(page.getHtml().xpath("//div[@class=\"articleList\"]").links().regex("").all());
            page.addTargetRequests(page.getHtml().links().regex(URL_LIST).all());
            //文章页
        } else {
            page.putField("title", page.getHtml().xpath("//div[@class='articalTitle']/h2"));
            page.putField("content", page.getHtml().xpath("//div[@id='articlebody']//div[@class='articalContent']"));
            page.putField("date", page.getHtml().xpath("//div[@id='articlebody']//span[@class='time SG_txtc']").regex("\\((.*)\\)"));
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

    private static Request getDetailRequest( String detailId ) {

        Request request = new Request(String.format(URL_DETAIL, detailId));
        request.setMethod( HttpConstant.Method.GET );
        return request;
    }
}
