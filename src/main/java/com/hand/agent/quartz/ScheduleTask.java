package com.hand.agent.quartz;

import com.hand.agent.confg.ExecutorConfig;
import com.hand.agent.confg.WatchServiceImpl;
import com.hand.agent.util.CsvReadUtil;
import com.hand.agent.util.ExcelReadUtil;
import com.hand.agent.util.KafkaProduceUtil;
import com.hand.agent.util.TxtReadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.hand.agent.util.CsvReadUtil.CSV_START;
import static com.hand.agent.util.ExcelReadUtil.HEADER;
import static com.hand.agent.util.ExcelReadUtil.XLSX_START;
import static com.hand.agent.util.ExcelReadUtil.XLS_START;
import static com.hand.agent.util.TxtReadUtil.TXT_START;

@Component
public class ScheduleTask {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleTask.class);
    private final CsvReadUtil csvReadUtil;
    private final ExcelReadUtil excelReadUtil;
    private final TxtReadUtil txtReadUtil;

    @Value("${dirctory.path}")
    private String dPath;

    @Autowired
    private ExecutorConfig config;
    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    public static String FILE_QUEUE = "FILE-QUEUE";

    public static String HEADER_NUM = "HEADER-NUM";

    public static String SHEET_NUM = "SHEET-NUM";

    public static String PRIMARY_KEYS = "PRIMARY-KEYS";

    private final WatchServiceImpl watchServiceimpl;
    private final KafkaProduceUtil kafkaProduceUtil;

    public ScheduleTask(CsvReadUtil csvReadUtil, ExcelReadUtil excelReadUtil, TxtReadUtil txtReadUtil, WatchServiceImpl watchServiceimpl, KafkaProduceUtil kafkaProduceUtil) {
        this.csvReadUtil = csvReadUtil;
        this.excelReadUtil = excelReadUtil;
        this.txtReadUtil = txtReadUtil;
        this.watchServiceimpl = watchServiceimpl;
        this.kafkaProduceUtil = kafkaProduceUtil;
    }

    @Async
    public void initFileRegiste() throws IOException {
        LOG.info("开始监听目录: {}", dPath);
        Path dir = Paths.get(dPath);
        kafkaProduceUtil.initProduceUtil();
        watchServiceimpl.walkAndRegisterDirectories(dir);
        watchServiceimpl.processEvents();
    }

    @Async
    void startFile(String uri) {
        File temp = new File(uri);
        LOG.info("{}：文件解析", temp.getName());
        if (uri.endsWith(".csv")) {
            csvReadUtil.read(temp);
        }

        if (uri.endsWith(".xls")) {
            excelReadUtil.read(temp, XLS_START);
        }

        if (uri.endsWith(".txt")) {
            txtReadUtil.read(temp);
        }

        if (uri.endsWith(".xlsx")) {
            excelReadUtil.read(temp, XLSX_START);
        }
    }

    @Async
    void readTemplateFile(String uri) {
        File temp = new File(uri);
        LOG.info("{}：解析模板配置", temp.getName());
        try {
            List<String> lines = Files.readAllLines(Paths.get(temp.getPath()));
            for (String line : lines.subList(1, lines.size())) {
                String[] records = line.split(",");
                List<String> temLines = Files.readAllLines(Paths.get(dPath + File.separator + records[1].trim()));
                if (records[0].toLowerCase().endsWith(".xlsx")) {
                    redisTemplate.opsForHash().put(XLSX_START, records[0].trim() + HEADER, temLines.get(0));
                    redisTemplate.opsForHash().put(XLSX_START, records[0].trim() + SHEET_NUM, records[2]);
                    redisTemplate.opsForHash().put(XLSX_START, records[0].trim() + HEADER_NUM, records[3]);
                    redisTemplate.opsForHash().put(XLSX_START, records[0].trim() + PRIMARY_KEYS, records[4].replaceAll("\\|",","));

                }
                if (records[0].toLowerCase().endsWith(".csv")) {
                    redisTemplate.opsForHash().put(CSV_START, records[0].trim() + HEADER, temLines.get(0));
                    redisTemplate.opsForHash().put(CSV_START, records[0].trim() + HEADER_NUM, records[3]);
                    redisTemplate.opsForHash().put(CSV_START, records[0].trim() + PRIMARY_KEYS, records[4].replaceAll("\\|",","));
                }
                if (records[0].toLowerCase().endsWith(".xls")) {
                    redisTemplate.opsForHash().put(XLS_START, records[0].trim() + HEADER, temLines.get(0));
                    redisTemplate.opsForHash().put(XLS_START, records[0].trim() + SHEET_NUM, records[2]);
                    redisTemplate.opsForHash().put(XLS_START, records[0].trim() + HEADER_NUM, records[3]);
                    redisTemplate.opsForHash().put(XLS_START, records[0].trim() + PRIMARY_KEYS, records[4].replaceAll("\\|",","));

                }
                if (records[0].toLowerCase().endsWith(".txt")) {
                    redisTemplate.opsForHash().put(TXT_START, records[0].trim() + HEADER, temLines.get(0));
                    redisTemplate.opsForHash().put(TXT_START, records[0].trim() + HEADER_NUM, records[3]);
                    redisTemplate.opsForHash().put(TXT_START, records[0].trim() + PRIMARY_KEYS, records[4].replaceAll("\\|",","));

                }
            }
        } catch (IOException e) {
            LOG.error("异常", e);
        }
    }
}
