package com.yunheng.scrapy.pipeline;

import jxl.CellView;
import jxl.Workbook;
import jxl.format.Colour;
import jxl.write.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.utils.FilePersistentBase;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 数据输出到Excel文件
 *
 * @author lanceliu <liuyunfei@yuntujinfu.com>
 * @date 16/7/8
 */
public class OneExcelPipeline  extends FilePersistentBase implements Pipeline,Closeable {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private WritableWorkbook wwb;

    private WritableCellFormat writableCommonCellFormat;
    private WritableCellFormat writableWrapCellFormat;
    /**
     * create a FilePipeline with default path"/data/webmagic/"
     */
    public OneExcelPipeline() {
        this("data/webmagic/", null, "sheet");
    }

    public OneExcelPipeline(String path, List< String> fieldList, String sheetName )  {
        setPath(path);
        try {
            wwb = Workbook.createWorkbook( new FileOutputStream(getFile(path)));
            WritableSheet sheet = wwb.createSheet(sheetName, 0);

            WritableFont fonts= new WritableFont(WritableFont.createFont("宋体"),10,WritableFont.BOLD);
            fonts.setColour(Colour.WHITE);
            WritableCellFormat writableCellFormats = new WritableCellFormat(fonts);
            writableCellFormats.setBackground(Colour.BLUE);

            CellView cellView = new CellView();
            cellView.setAutosize(true); //设置自动大小

            // 填充表头
            for (int i = 0; i < fieldList.size(); i++) {
                Label label = new Label(i, 0, fieldList.get(i), writableCellFormats);
                sheet.addCell(label);
                sheet.setColumnView(i, cellView);//根据内容自动设置列宽
            }


            WritableFont font= new WritableFont(WritableFont.createFont("宋体"),10,WritableFont.NO_BOLD);
            writableCommonCellFormat = new WritableCellFormat(font);
            writableCommonCellFormat.setBackground(Colour.WHITE);

            writableWrapCellFormat = new WritableCellFormat(font);
            writableWrapCellFormat.setBackground(Colour.WHITE);
            writableWrapCellFormat.setWrap(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void process(ResultItems resultItems, Task task) {
        Map<String, Object>  map = resultItems.getAll();

        if ( map.entrySet().size() > 0 ) {
            WritableSheet sheet = wwb.getSheet(0);
            int rowNum = sheet.getRows() ;
            try {
                Label label_1 = new Label(0, rowNum, rowNum+"", writableCommonCellFormat);
                Label label_2 = new Label(1, rowNum, map.get("managerName").toString(), writableCommonCellFormat);
                Label label_3 = new Label(2, rowNum, map.get("artificialPersonName").toString(), writableCommonCellFormat);
                Label label_4 = new Label(3, rowNum, map.get("primaryInvestType").toString(), writableCommonCellFormat);
                Label label_5 = new Label(4, rowNum, map.get("applyBizType").toString(), writableCommonCellFormat);
                Label label_6 = new Label(5, rowNum, map.get("city").toString(), writableCommonCellFormat);
                Label label_7 = new Label(6, rowNum, map.get("registerNo").toString(), writableCommonCellFormat);
                Label label_8 = new Label(7, rowNum, map.get("establishDate").toString(), writableCommonCellFormat);
                Label label_9 = new Label(8, rowNum, map.get("registerDate").toString(), writableCommonCellFormat);
                Label label_10 = new Label(9, rowNum, map.get("registerAddress").toString(), writableCommonCellFormat);
                Label label_11 = new Label(10, rowNum, map.get("officeAddress").toString(), writableCommonCellFormat);
                Label label_12 = new Label(11, rowNum, map.get("orgSiteUrl").toString(), writableCommonCellFormat);
                Label label_13 = new Label(12, rowNum, map.get("recordedFund").toString(), writableWrapCellFormat);

                sheet.addCell(label_1);
                sheet.addCell(label_2);
                sheet.addCell(label_3);
                sheet.addCell(label_4);
                sheet.addCell(label_5);
                sheet.addCell(label_6);
                sheet.addCell(label_7);
                sheet.addCell(label_8);
                sheet.addCell(label_9);
                sheet.addCell(label_10);
                sheet.addCell(label_11);
                sheet.addCell(label_12);
                sheet.addCell(label_13);

            } catch (Exception e) {
                logger.error("", e);
            }
        }

    }

    @Override
    public void close() throws IOException {
        wwb.write();
        try {
            wwb.close();
        } catch (WriteException e) {
            logger.error("", e);
        }
    }
}
