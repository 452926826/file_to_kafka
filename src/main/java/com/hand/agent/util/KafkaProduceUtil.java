package com.hand.agent.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;
import java.util.concurrent.Future;

@Configuration
public class KafkaProduceUtil {
    private Logger logger = LoggerFactory.getLogger(KafkaProduceUtil.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String servers;

    @Value("${spring.kafka.producer.acks}")
    private String acks;

    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;

    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;

    @Value("${spring.kafka.producer.retries}")
    private String retries;

    @Value("${spring.kafka.producer.batch-size}")
    private String batchSize;

    private KafkaProducer<String, String> producer;

    public void initProduceUtil() {
        Properties props = new Properties();
        props.put("metadata.broker.list", servers);
        props.put("bootstrap.servers", servers);
        props.put("acks", acks);
        props.put("retries", retries);
        props.put("batch.size", batchSize);
        props.put("linger.ms", "100");
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);
        this.producer = new KafkaProducer<>(props);
    }

    public void sendMessage(ProducerRecord record){
        Future<RecordMetadata> metadataFuture = this.producer.send(record, (recordMetadata, e) -> logger.info(String.valueOf(recordMetadata.offset())));
    }
}
