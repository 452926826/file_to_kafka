package com.hand.agent.quartz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class StartTask {
    private Logger logger = LoggerFactory.getLogger(StartTask.class);
    @Autowired
    private ScheduleTask task;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;


    @PostConstruct
    private void init() throws Exception {
        logger.info("开始监听目录变化");
        task.initFileRegiste();
        logger.info("开始启动线程同步");
        new Thread(new TaskThread(task, redisTemplate)).start();
    }
}
