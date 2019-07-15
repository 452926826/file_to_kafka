package com.hand.agent.util;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hand.agent.quartz.ScheduleTask.HEADER_NUM;
import static com.hand.agent.quartz.ScheduleTask.PRIMARY_KEYS;

@Component
public class CsvReadUtil {
    private Logger logger = LoggerFactory.getLogger(CsvReadUtil.class);
    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private KafkaProduceUtil kafkaProduceUtil;

    @Value("${spring.kafka.template.default-topic}")
    private String TOPIC;

    public static String CSV_START = "CSV";

    public String HEADER = "-HEADER";

    @Value("${exist.header}")
    private Boolean existHeader;

/*    @Value("${primary.keys}")
    private String primaryKeys;*/

/*    @Value("${header.line-num}")
    private Integer headerLineNum;*/

    public void read(File csv) {
        logger.info("解析csv文件");
        try {
            List<String> lines = Files.readAllLines(Paths.get(csv.getPath()));
            if (lines.isEmpty()) {
                return;
            }
            //解析第一行为字段名
         /*   if (existHeader) {
                Object header = redisTemplate.opsForHash().get(CSV_START, csv.getName() + HEADER);
                if (header == null) {
                    redisTemplate.opsForHash().put(CSV_START, csv.getName() + HEADER, lines.get(headerLineNum));
                }
            }*/
            Integer headerLineNum = Integer.parseInt(redisTemplate.opsForHash().get(CSV_START, csv.getName() + HEADER_NUM).toString());
            Object obj = redisTemplate.opsForHash().get(CSV_START, csv.getName());
            int num = Integer.parseInt((obj == null ? 0 : obj).toString());
            if (lines.size() == (headerLineNum + 1) && existHeader) {
                return;
            }
            if (num < lines.size()) {
                if(existHeader) {
                    product(lines.subList(num + headerLineNum + 1, lines.size()), num, csv.getName());
                }else{
                    product(lines.subList(num, lines.size()), num, csv.getName());
                }
            }
        } catch (IOException e) {
            logger.error("错误：",e.getMessage());
        }
    }

    private void product(List<String> strings, int num, String csv) {
        int tmp = num;
        String[] header = redisTemplate.opsForHash().get(CSV_START, csv + HEADER).toString().split(",");
        String[] keys = redisTemplate.opsForHash().get(CSV_START, csv + PRIMARY_KEYS).toString().split(",");
        for (String record : strings) {
            if(record.length()==0){
                return;
            }
            StringBuilder id = new StringBuilder();
            String[] records = record.split(",");
            for (String key : keys) {
                id.append(records[Integer.parseInt(key)]);
                id.append("-");
            }
            ProducerRecord<String, String> producerRecord = null;
            if (existHeader) {
                Map<String, String> tmpMap = new HashMap<>();
                tmpMap.put("table",csv);
                for (int i = 0; i < records.length; i++) {
                    tmpMap.put(header[i], records[i]);
                }
                producerRecord = new ProducerRecord<String, String>(TOPIC, id.toString(), JSON.toJSONString(tmpMap));
            } else {
                StringBuilder tmpJson = new StringBuilder();
                tmpJson.append("{")
                        .append(record)
                        .append("}");
                producerRecord = new ProducerRecord<String, String>(TOPIC, id.toString(), tmpJson.toString());
            }
            kafkaProduceUtil.sendMessage(producerRecord);
            tmp = tmp + 1;
            redisTemplate.opsForHash().put(CSV_START, csv, String.valueOf(tmp));
        }
    }
}
