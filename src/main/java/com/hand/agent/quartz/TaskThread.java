package com.hand.agent.quartz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import static com.hand.agent.confg.WatchServiceImpl.TEMPLATE_QUEUE;
import static com.hand.agent.quartz.ScheduleTask.FILE_QUEUE;

@Component
public class TaskThread implements Runnable {
    private Logger logger = LoggerFactory.getLogger(TaskThread.class);
    private final ScheduleTask task;
    private  final RedisTemplate<String, String> redisTemplate;

    public TaskThread(ScheduleTask task, RedisTemplate<String, String> redisTemplate) {
        this.task = task;
        this.redisTemplate = redisTemplate;
    }
    @Override
    public void run() {
        logger.info("进入线程,读取redis 队列数据");
        while (true) {
            String uri = redisTemplate.opsForSet().pop(FILE_QUEUE);
            String template_uri = redisTemplate.opsForSet().pop(TEMPLATE_QUEUE);
            if (uri == null) {
                try {
                    logger.info("队列没有数据，开始睡眠");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.error("错误：",e.getMessage());
                }
            } else {
                task.startFile(uri);
            }
            if (template_uri == null) {
                try {
                    logger.info("模板队列没有数据，开始睡眠");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.error("错误：",e.getMessage());
                }
            } else {
                task.readTemplateFile(template_uri);
            }
        }
    }
}
