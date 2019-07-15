package com.hand.agent.controllers;

import com.hand.agent.util.CsvReadUtil;
import com.hand.agent.util.ExcelReadUtil;
import com.hand.agent.util.TxtReadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@RestController
public class ApiController {
    private Logger logger = LoggerFactory.getLogger(ApiController.class);
    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @RequestMapping("/query/agent/info")
    public Map<String,String> agentInfo(HttpServletRequest request, HttpServletResponse response){
        Map<String,String> tmp = new HashMap<>();
        InetAddress ia=null;
        try {
            ia=ia.getLocalHost();
            tmp.put("ip",ia.getHostAddress());
        } catch (UnknownHostException e) {
            tmp.put("ip","错误");
        }
        tmp.put("status","up");
        AtomicReference<Integer> xls= new AtomicReference<>(0);
        logger.info("获取xls已经传输的数据");
        redisTemplate.opsForHash().entries(ExcelReadUtil.XLS_START).forEach((key, value)->{
            if(key.toString().toLowerCase().endsWith(".xls")){
                xls.updateAndGet(v -> v + Integer.parseInt(value.toString()));
            }
        });
        tmp.put("xls", String.valueOf(xls));
        AtomicReference<Integer> xlsx= new AtomicReference<>(0);
        logger.info("获取xlsx已经传输的数据");
        redisTemplate.opsForHash().entries(ExcelReadUtil.XLSX_START).forEach((key, value)->{
            if(key.toString().toLowerCase().endsWith(".xlsx")){
                xlsx.updateAndGet(v -> v + Integer.parseInt(value.toString()));
            }
        });
        tmp.put("xlsx", String.valueOf(xlsx));
        AtomicReference<Integer> txt= new AtomicReference<>(0);
        logger.info("获取txt已经传输的数据");
        redisTemplate.opsForHash().entries(TxtReadUtil.TXT_START).forEach((key, value)->{
            if(key.toString().toLowerCase().endsWith(".txt")){
                txt.updateAndGet(v -> v + Integer.parseInt(value.toString()));
            }
        });
        tmp.put("txt", String.valueOf(txt));
        AtomicReference<Integer> csv= new AtomicReference<>(0);
        logger.info("获取csv已经传输的数据");
        redisTemplate.opsForHash().entries(CsvReadUtil.CSV_START).forEach((key, value)->{
            if(key.toString().toLowerCase().endsWith(".csv")){
                csv.updateAndGet(v -> v + Integer.parseInt(value.toString()));
            }
        });
        tmp.put("csv", String.valueOf(csv));
        return tmp;
    }
}
