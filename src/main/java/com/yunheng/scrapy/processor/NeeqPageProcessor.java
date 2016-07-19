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
import us.codecraft.webmagic.selector.Json;
import us.codecraft.webmagic.utils.HttpConstant;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: 下载需要多线程; 下载和打印转移到pipeline中去
 *
 * @author lanceliu <liuyunfei@yuntujinfu.com>
 * @date 16/7/6
 */
public class NeeqPageProcessor implements PageProcessor {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private Site site = Site.me().setDomain("www.neeq.com.cn")
            .addHeader("Content-Type", "application/x-www-form-urlencoded").setRetryTimes(2);
    public static final String URL_START_URL = "http://www.neeq.com.cn/disclosureInfoController/infoResult.do?test=%s";
    public static final String URL_LIST = "http://www.neeq.com.cn/disclosureInfoController/infoResult.do";
    public static final String URL_DETAIL = "http://www.neeq.com.cn%s";
    public static String PATH_SEPERATOR = "/";

    public void process(Page page) {
        if (page.getUrl().regex(URL_LIST).match()) {
            String rawTxt = page.getRawText().replaceFirst("null\\(\\[","").replaceAll("\\]\\)$", "");
            Json jsonObj = new Json(rawTxt);

            jsonObj.jsonPath("$.listInfo.content[*]").nodes().forEach( node->{
                JSONObject json = JSON.parseObject( node.get() );
                downloadFile(String.format(URL_DETAIL, json.getString("destFilePath")), json.getString("destFilePath"));
                logger.info("{},{},{},{},{}", json.getString("companyCd"),json.getString("disclosureTitle"),
                        json.getString("destFilePath"),json.getString("companyName"),"1".equals(json.getString("xxfcbj")) ? "创":"基");

            });

            try {
                if ( !Boolean.parseBoolean(  jsonObj.jsonPath("$.listInfo.lastPage").get()  ) ) {
                    int pageNum = Integer.parseInt(  jsonObj.jsonPath("$.listInfo.number").get() ) + 1;
                    page.addTargetRequest( getListRequest(pageNum) );
                }
            } catch ( Exception e ) {
                logger.warn("", e);
            }

        }
    }

    public void downloadFile(String picUrl, String savePath) {
        try {
            URL url = new URL(picUrl);
            DataInputStream dis = new DataInputStream(url.openStream());
            FileOutputStream fos = new FileOutputStream(getFile("data/webmagic"+savePath));

            byte[] buffer = new byte[1024];
            int length;
            while(-1 != (length = dis.read(buffer))){
                fos.write(buffer,0,length);
            }
            fos.flush();
            fos.close();
            dis.close();
        } catch ( Exception e ) {
            logger.error("下载文件时出错, 下载地址{}, 异常信息{}", picUrl, e);
        }
    }

    public File getFile(String fullName) {
        checkAndMakeParentDirecotry(fullName);
        return new File(fullName);
    }

    public void checkAndMakeParentDirecotry(String fullName) {
        int index = fullName.lastIndexOf(PATH_SEPERATOR);
        if (index > 0) {
            String path = fullName.substring(0, index);
            File file = new File(path);
            if (!file.exists()) {
                file.mkdirs();
            }
        }
    }

    public Site getSite() {
        return site;
    }

    public static void main(String[] args)  {
        Spider.create(new NeeqPageProcessor()).setDownloader( new JSONRequestDownloader() ).thread(1).addRequest( getListRequest(0) ).run();
    }

    private static Request getListRequest( int pageNum  ) {
        if ( pageNum < 0 ) pageNum = 0;
        Request request = new Request(String.format(URL_START_URL, pageNum));
        request.setMethod( HttpConstant.Method.POST );
        Map<String,String> maps = new HashMap<>();
        maps.put("disclosureType", "5");
        maps.put("page", pageNum+"" );
        maps.put("companyCd", "");
        maps.put("isNewThree", "1");
        maps.put("startTime", "2011-01-01");
        maps.put("endTime", "2016-07-19");
        maps.put("keyword", "质押公告");
        maps.put("xxfcbj", "");
        request.putExtra("postParams", maps);

        return request;
    }
}
