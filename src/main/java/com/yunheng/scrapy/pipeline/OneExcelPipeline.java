package com.yunheng.scrapy.pipeline;

import jxl.CellView;
import jxl.Workbook;
import jxl.format.Colour;
import jxl.write.*;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
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

    private SXSSFWorkbook workbook;
    private CellStyle style2;

    private String fullName;
    /**
     * create a FilePipeline with default path"/data/webmagic/"
     */
    public OneExcelPipeline() {
        this("data/webmagic/", null, "sheet");
    }

    public OneExcelPipeline(String path, List< String> fieldList, String sheetName )  {
        fullName = path;
        generateSheetWithPOI(fieldList, sheetName);
    }

    @Override
    public synchronized void process(ResultItems resultItems, Task task) {
        fillOutWithPOI(resultItems.getAll());
    }

    private void generateSheetWithPOI(List< String> fieldList, String sheetName) {
        // 声明一个工作薄
        workbook = new SXSSFWorkbook(4);
        // 生成一个表格
        Sheet sheet = workbook.createSheet( sheetName );

        // 设置表格默认列宽度为15个字节
        sheet.setDefaultColumnWidth((short) 18);
        // 生成一个样式
        CellStyle style = workbook.createCellStyle();
        // 设置这些样式
        style.setFillForegroundColor(HSSFColor.SKY_BLUE.index);
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        style.setBorderBottom(CellStyle.BORDER_THIN);
        style.setBorderLeft(CellStyle.BORDER_THIN);
        style.setBorderRight(CellStyle.BORDER_THIN);
        style.setBorderTop(CellStyle.BORDER_THIN);
        style.setAlignment(CellStyle.ALIGN_CENTER);
        // 生成一个字体
        org.apache.poi.ss.usermodel.Font font = workbook.createFont();
        font.setColor(HSSFColor.VIOLET.index);
        font.setFontHeightInPoints((short) 12);
        font.setBoldweight(Font.BOLDWEIGHT_BOLD);
        // 把字体应用到当前的样式
        style.setFont(font);
        // 生成并设置另一个样式
        style2 = workbook.createCellStyle();
        style2.setFillForegroundColor(HSSFColor.LIGHT_YELLOW.index);
        style2.setFillPattern(CellStyle.SOLID_FOREGROUND);
        style2.setBorderBottom(CellStyle.BORDER_THIN);
        style2.setBorderLeft(CellStyle.BORDER_THIN);
        style2.setBorderRight(CellStyle.BORDER_THIN);
        style2.setBorderTop(CellStyle.BORDER_THIN);
        style2.setAlignment(CellStyle.ALIGN_CENTER);
        style2.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
        style2.setWrapText(true);


        // 生成另一个字体
        org.apache.poi.ss.usermodel.Font font2 = workbook.createFont();
        font2.setBoldweight(Font.BOLDWEIGHT_NORMAL);
        // 把字体应用到当前的样式
        style2.setFont(font2);

        Row row = sheet.createRow(0);
        for (int i = 0; i < fieldList.size(); i++) {
            Cell cell = row.createCell(i);
            cell.setCellStyle(style);
            cell.setCellValue(fieldList.get(i));
        }

    }
    private void fillOutWithPOI(Map<String, Object>  map) {
        if ( map.entrySet().size() > 0 ) {
            SXSSFSheet sheet = workbook.getSheetAt(0);
            Row row = sheet.createRow(sheet.getLastRowNum()+1);
            try {
                Cell cell_0 = row.createCell(0);
                Cell cell_1 = row.createCell(1);
                Cell cell_2 = row.createCell(2);
                Cell cell_3 = row.createCell(3);
                Cell cell_4 = row.createCell(4);
                Cell cell_5 = row.createCell(5);
                Cell cell_6 = row.createCell(6);
                Cell cell_7 = row.createCell(7);
                Cell cell_8 = row.createCell(8);
                Cell cell_9 = row.createCell(9);
                Cell cell_10 = row.createCell(10);
                Cell cell_11 = row.createCell(11);
                Cell cell_12 = row.createCell(12);

                cell_0.setCellStyle( style2 );
                cell_1.setCellStyle( style2 );
                cell_2.setCellStyle( style2 );
                cell_3.setCellStyle( style2 );
                cell_4.setCellStyle( style2 );
                cell_5.setCellStyle( style2 );
                cell_6.setCellStyle( style2 );
                cell_7.setCellStyle( style2 );
                cell_8.setCellStyle( style2 );
                cell_9.setCellStyle( style2 );
                cell_10.setCellStyle( style2 );
                cell_11.setCellStyle( style2 );
                cell_12.setCellStyle( style2 );

                cell_0.setCellValue(row.getRowNum());
                cell_1.setCellValue(map.get("managerName") == null ? "": map.get("managerName").toString());
                cell_2.setCellValue( map.get("artificialPersonName") == null ? "": map.get("artificialPersonName").toString() );
                cell_3.setCellValue( map.get("primaryInvestType") == null ? "": map.get("primaryInvestType").toString() );
                cell_4.setCellValue( map.get("applyBizType") == null ? "": map.get("applyBizType").toString() );
                cell_5.setCellValue( map.get("city") == null ? "": map.get("city").toString() );
                cell_6.setCellValue( map.get("registerNo") == null ? "": map.get("registerNo").toString() );
                cell_7.setCellValue( map.get("establishDate") == null ? "": map.get("establishDate").toString() );
                cell_8.setCellValue( map.get("registerDate") == null ? "": map.get("registerDate").toString() );
                cell_9.setCellValue( map.get("registerAddress") == null ? "": map.get("registerAddress").toString() );
                cell_10.setCellValue( map.get("officeAddress") == null ? "": map.get("officeAddress").toString() );
                cell_11.setCellValue( map.get("orgSiteUrl") == null ? "": map.get("orgSiteUrl").toString() );

                cell_12.setCellValue( map.get("recordedFund") == null ?
                        "": map.get("recordedFund").toString() );

            } catch (Exception e) {
                logger.error("", e);
            }
            logger.info(java.lang.Runtime.getRuntime().freeMemory() + " bytes");
        }
    }

    private void generateSheetWithJXL(String path, List< String> fieldList, String sheetName) {
        try {
            wwb = Workbook.createWorkbook( new FileOutputStream(getFile(path)) );
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

    private void fillOutWithJXL(Map<String, Object>  map) {
        if ( map.entrySet().size() > 0 ) {
            WritableSheet sheet = wwb.getSheet(0);
            int rowNum = sheet.getRows() ;
            try {
                Label label_1 = new Label(0, rowNum, rowNum+"", writableCommonCellFormat);
                Label label_2 = new Label(1, rowNum, map.get("managerName") == null ? "": map.get("managerName").toString(), writableCommonCellFormat);
                Label label_3 = new Label(2, rowNum, map.get("artificialPersonName") == null ? "": map.get("artificialPersonName").toString(), writableCommonCellFormat);
                Label label_4 = new Label(3, rowNum, map.get("primaryInvestType") == null ? "": map.get("primaryInvestType").toString(), writableCommonCellFormat);
                Label label_5 = new Label(4, rowNum, map.get("applyBizType") == null ? "": map.get("applyBizType").toString(), writableCommonCellFormat);
                Label label_6 = new Label(5, rowNum, map.get("city") == null ? "": map.get("city").toString(), writableCommonCellFormat);
                Label label_7 = new Label(6, rowNum, map.get("registerNo") == null ? "": map.get("registerNo").toString(), writableCommonCellFormat);
                Label label_8 = new Label(7, rowNum, map.get("establishDate") == null ? "": map.get("establishDate").toString(), writableCommonCellFormat);
                Label label_9 = new Label(8, rowNum, map.get("registerDate") == null ? "": map.get("registerDate").toString(), writableCommonCellFormat);
                Label label_10 = new Label(9, rowNum, map.get("registerAddress") == null ? "": map.get("registerAddress").toString(), writableCommonCellFormat);
                Label label_11 = new Label(10, rowNum, map.get("officeAddress") == null ? "": map.get("officeAddress").toString(), writableCommonCellFormat);
                Label label_12 = new Label(11, rowNum, map.get("orgSiteUrl") == null ? "": map.get("orgSiteUrl").toString(), writableCommonCellFormat);
                Label label_13 = new Label(12, rowNum, map.get("recordedFund") == null ? "": map.get("recordedFund").toString(), writableWrapCellFormat);

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
            logger.info(java.lang.Runtime.getRuntime().freeMemory() + " bytes");
        }
    }

//    @Override
//    public void close() throws IOException {
//        wwb.write();
//        try {
//            wwb.close();
//        } catch (WriteException e) {
//            logger.error("", e);
//        }
//    }
    @Override
    public void close() throws IOException {
        try {

            FileOutputStream out = new FileOutputStream(fullName);
            workbook.write(out);
            out.close();

            // dispose of temporary files backing this workbook on disk
            workbook.dispose();
        } catch (Exception e) {
            logger.error("", e);
        }
    }
}
