package com.hand.agent.listener;

import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Configuration
public class Excel03Listener extends AnalysisEventListener<List<String>> {
    private Logger logger = LoggerFactory.getLogger(Excel03Listener.class);

    private List<List<String>>data = new ArrayList<>();

    @Override
    public void invoke(List<String> o, AnalysisContext context) {
        data.add(o);
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext analysisContext) {
        logger.info("处理完成" + new Date());
    }

    public List<List<String>> getData() {
        return data;
    }
}
