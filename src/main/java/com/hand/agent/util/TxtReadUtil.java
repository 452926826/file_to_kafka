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
public class TxtReadUtil {
    private Logger logger = LoggerFactory.getLogger(TxtReadUtil.class);
    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private KafkaProduceUtil kafkaProduceUtil;

    @Value("${separate.char}")
    private String sChar;

    @Value("${spring.kafka.template.default-topic}")
    private String TOPIC;

    public static String TXT_START = "TXT";

    public String HEADER = "-HEADER";

    @Value("${exist.header}")
    private Boolean existHeader;

/*    @Value("${primary.keys}")
    private String primaryKeys;*/

/*    @Value("${header.line-num}")
    private Integer headerLineNum;*/

    public void read(File txt) {
        logger.info("解析txt文件");
        try {
            List<String> lines = Files.readAllLines(Paths.get(txt.getPath()));
            if (lines.isEmpty()) {
                return;
            }
            //解析第一行为字段名
            /*if (existHeader) {
                Object header = redisTemplate.opsForHash().get(TXT_START, txt.getName() + HEADER);
                if (header == null) {
                    redisTemplate.opsForHash().put(TXT_START, txt.getName() + HEADER, lines.get(headerLineNum));
                }
            }*/
            Integer headerLineNum = Integer.parseInt(redisTemplate.opsForHash().get(TXT_START, txt.getName() + HEADER_NUM).toString());
            Object obj = redisTemplate.opsForHash().get(TXT_START, txt.getName());
            logger.info("读取txt文件");
            int num = Integer.parseInt((obj == null ? 0 : obj).toString());
            if (lines.size() == (headerLineNum + 1) && existHeader) {
                return;
            }
            if (num < lines.size()) {
                if(existHeader) {
                    product(lines.subList(num + headerLineNum + 1, lines.size()), num, txt.getName());
                }else{
                    product(lines.subList(num, lines.size()), num, txt.getName());
                }
            }
        } catch (IOException e) {
            logger.error("错误：",e.getMessage());
        }
    }

    private void product(List<String> strings, int num, String txt) {
        int tmp = num;
        String[] header = redisTemplate.opsForHash().get(TXT_START, txt + HEADER).toString().split(",");
        String[] keys = redisTemplate.opsForHash().get(TXT_START, txt + PRIMARY_KEYS).toString().split(",");
        for (String record : strings) {
            if(record.length()==0){
                return;
            }
            String[] records = record.split(sChar);
            StringBuilder id = new StringBuilder();
            for (String key : keys) {
                id.append(records[Integer.parseInt(key)]);
                id.append("-");
            }
            ProducerRecord<String, String> producerRecord = null;
            if (existHeader) {
                Map<String, String> tmpObj = new HashMap<>();
                tmpObj.put("table",txt);
                for (int i = 0; i < records.length; i++) {
                    tmpObj.put(header[i], records[i]);
                }
                producerRecord = new ProducerRecord<String, String>(TOPIC, id.toString(), JSON.toJSONString(tmpObj));
            } else {
                StringBuilder tmpJson = new StringBuilder();
                tmpJson.append("{").append(record).append("}");
                producerRecord = new ProducerRecord<String, String>(TOPIC, id.toString(), tmpJson.toString());
            }
            kafkaProduceUtil.sendMessage(producerRecord);
            tmp = tmp + 1;
            redisTemplate.opsForHash().put(TXT_START, txt, String.valueOf(tmp));
        }
    }
}
