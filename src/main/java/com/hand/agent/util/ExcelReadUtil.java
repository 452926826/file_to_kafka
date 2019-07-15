package com.hand.agent.util;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.metadata.Sheet;
import com.alibaba.fastjson.JSON;
import com.hand.agent.listener.Excel03Listener;
import com.hand.agent.listener.ExcelListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hand.agent.quartz.ScheduleTask.HEADER_NUM;
import static com.hand.agent.quartz.ScheduleTask.PRIMARY_KEYS;
import static com.hand.agent.quartz.ScheduleTask.SHEET_NUM;

@Component
public class ExcelReadUtil {
    private Logger logger = LoggerFactory.getLogger(CsvReadUtil.class);

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private KafkaProduceUtil kafkaProduceUtil;

    @Value("${spring.kafka.template.default-topic}")
    private String TOPIC;
    public static String XLSX_START = "XLSX";
    public static String XLS_START = "XLS";

    public static String HEADER = "-HEADER";

    @Value("${exist.header}")
    private Boolean existHeader;
/*
    @Value("${header.line-num}")
    private Integer headerLineNum;*/

/*    @Value("${sheet.num}")
    private Integer sheetNum;*/
/*
    @Value("${primary.keys}")
    private String primaryKeys;*/

    public void read(File csv, String type) {
        logger.info("解析excel文件");
        try {
            FileInputStream in = new FileInputStream(csv);
            if (type.equals(XLSX_START)) {
                Integer sheetNum = Integer.parseInt(redisTemplate.opsForHash().get(XLSX_START, csv.getName() + SHEET_NUM).toString());
                ExcelListener excelListener = new ExcelListener();
                EasyExcelFactory.readBySax(in, new Sheet(sheetNum, 0), excelListener);
                in.close();
                List<List<String>> list = excelListener.getData();
                if (list.isEmpty()) {
                    return;
                }
                //解析第一行为字段名
               /* if (existHeader) {
                    Object header = redisTemplate.opsForHash().get(XLSX_START, csv.getName() + HEADER);
                    if (header == null) {
                        redisTemplate.opsForHash().put(XLSX_START, csv.getName() + HEADER, StringUtils.join(list.get(headerLineNum), ","));
                    }
                }*/
                Integer headerLineNum = Integer.parseInt(redisTemplate.opsForHash().get(XLSX_START, csv.getName() + HEADER_NUM).toString());
                Object obj = redisTemplate.opsForHash().get(XLSX_START, csv.getName());
                int num = Integer.parseInt((obj == null ? 0 : obj).toString());
                if (list.size() == (headerLineNum+1) && existHeader) {
                    return;
                }
                if (num < list.size()) {
                    if(existHeader) {
                        product(list.subList(num + headerLineNum + 1, list.size()), num, csv.getName(), type);
                    }else{
                        product(list.subList(num, list.size()), num, csv.getName(), type);
                    }
                }
            } else {
                Integer sheetNum = Integer.parseInt(redisTemplate.opsForHash().get(XLS_START, csv.getName() + SHEET_NUM).toString());
                Excel03Listener excel03Listener = new Excel03Listener();
                BufferedInputStream bis = new BufferedInputStream(in);
                EasyExcelFactory.readBySax(bis, new Sheet(sheetNum, 0), excel03Listener);
                in.close();
                List<List<String>> list = excel03Listener.getData();
                if (list.isEmpty()) {
                    return;
                }
                //解析第一行为字段名
               /* if (existHeader) {
                    Object header = redisTemplate.opsForHash().get(XLS_START, csv.getName() + HEADER);
                    if (header == null) {
                        redisTemplate.opsForHash().put(XLS_START, csv.getName() + HEADER, StringUtils.join(list.get(headerLineNum), ","));
                    }
                }*/
                Integer headerLineNum = Integer.parseInt(redisTemplate.opsForHash().get(XLS_START, csv.getName() + HEADER_NUM).toString());
                Object obj = redisTemplate.opsForHash().get(XLS_START, csv.getName());
                int num = Integer.parseInt((obj == null ? 0 : obj).toString());
                if (list.size() == (headerLineNum+1) && existHeader) {
                    return;
                }
                if (num < list.size()) {
                    if(existHeader) {
                        product(list.subList(num + headerLineNum + 1, list.size()), num, csv.getName(), type);
                    }else{
                        product(list.subList(num, list.size()), num, csv.getName(), type);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("错误：",e.getMessage());
        }
    }

    public void product(List<List<String>> strings, int num, String csv, String type) {
        int tmp = num;
        String[] header = redisTemplate.opsForHash().get(type, csv + HEADER).toString().split(",");
        String[] keys = redisTemplate.opsForHash().get(type, csv + PRIMARY_KEYS).toString().split(",");
        for (List<String> records : strings) {
            if(records.size()==0){
                return;
            }
            StringBuilder id = new StringBuilder();
            for (String key : keys) {
                id.append(records.get(Integer.parseInt(key)));
                id.append("-");
            }
            ProducerRecord<String, String> record = null;
            if (existHeader) {
                Map<String, String> tmpObj = new HashMap<>();
                tmpObj.put("table",csv);
                for (int i = 0; i < records.size(); i++) {
                    if (header[i].toLowerCase().contains("date") || header[i].toLowerCase().contains("time")) {
                        tmpObj.put(header[i], DateUtil.getPOIDate(false, Double.parseDouble(records.get(i))));
                    } else {
                        tmpObj.put(header[i], records.get(i));
                    }
                }
                record = new ProducerRecord<String, String>(TOPIC, id.toString(), JSON.toJSONString(tmpObj));
            } else {
                StringBuilder tmpJson = new StringBuilder();
                tmpJson.append("{")
                        .append(StringUtils.join(records, ","))
                        .append("}");
                record = new ProducerRecord<String, String>(TOPIC, id.toString(), tmpJson.toString());
            }

            kafkaProduceUtil.sendMessage(record);
            tmp = tmp + 1;
            redisTemplate.opsForHash().put(type, csv, String.valueOf(tmp));
        }
    }

}
