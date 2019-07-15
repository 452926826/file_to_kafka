package com.hand.agent.confg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import static com.hand.agent.quartz.ScheduleTask.FILE_QUEUE;
import static java.nio.file.StandardWatchEventKinds.*;

@Component
public class WatchServiceImpl {

    private static final Logger LOG = LoggerFactory.getLogger(WatchServiceImpl.class);

    @Value("${dirctory.path}")
    private String dPath;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    public static String TEMPLATE_QUEUE = "TEMPLATE-QUEUE";


    private final WatchService watcher = FileSystems.getDefault().newWatchService();
    private final Map<WatchKey, Path> keys = new HashMap<>();

    public WatchServiceImpl() throws IOException {
    }

    private void registerDirectory(Path dir) throws IOException {
        LOG.info("Register directory: {}", dir);
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        keys.put(key, dir);
    }

    public void walkAndRegisterDirectories(Path start) throws IOException {
        LOG.info("Watch root directory: {}", start);
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                registerDirectory(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public void processEvents() {
        for (; ; ) {
            LOG.info("Wait for notify");
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }
            Path dir = keys.get(key);
            if (dir == null) {
                LOG.warn("Directory not registered");
                continue;
            }
            for (WatchEvent<?> event : key.pollEvents()) {
                @SuppressWarnings("rawtypes")
                WatchEvent.Kind kind = event.kind();
                // 事件内容
                @SuppressWarnings("unchecked")
                Path name = ((WatchEvent<Path>) event).context();
                Path child = dir.resolve(name);
                // 打印事件
                if (kind == ENTRY_MODIFY && name.toString().endsWith(".csv")) {
                    LOG.info("{} file change {}", event.kind().name(), child);
                    if (name.toString().toLowerCase().equals("template.csv")) {
                        redisTemplate.opsForSet().add(TEMPLATE_QUEUE, child.toUri().getPath());
                    } else {
                        redisTemplate.opsForSet().add(FILE_QUEUE, child.toUri().getPath());
                    }
                }

                if (kind == ENTRY_MODIFY && name.toString().endsWith(".xls")) {
                    LOG.info("{} file change {}", event.kind().name(), child);
                    redisTemplate.opsForSet().add(FILE_QUEUE, child.toUri().getPath());
                }

                if (kind == ENTRY_MODIFY && name.toString().endsWith(".txt")) {
                    LOG.info("{} file change {}", event.kind().name(), child);
                    redisTemplate.opsForSet().add(FILE_QUEUE, child.toUri().getPath());
                }

                if ((kind == ENTRY_MODIFY) && name.toString().endsWith(".xlsx") && !name.toString().startsWith("~$")) {
                    LOG.info("{} file change {}", event.kind().name(), child);
                    redisTemplate.opsForSet().add(FILE_QUEUE, child.toUri().getPath());
                }

                // 如果新的文件夹被创建那么注册到监听中
                if (kind == ENTRY_CREATE) {
                    try {
                        if (Files.isDirectory(child)) {
                            walkAndRegisterDirectories(child);
                        }
                    } catch (IOException e) {
                        LOG.error("异常", e);
                    }
                }
            }
            // 如果目录不可访问 那么重置keys 移除不可访问的key
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);
                // all directories are inaccessible
                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }

}
