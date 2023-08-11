package com.gameplus.kinesis.repository;

import com.gameplus.kinesis.processor.Processor;
import com.gameplus.kinesis.utils.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaConsumer {
    private final String streamName;
    private static final Logger log = LogManager.getLogger(KafkaConsumer.class);

    public KafkaConsumer(String streamName) {
        this.streamName = streamName;
    }

    public void startConsume() {

        Properties props = PropertyUtil.getPropertiesByPrefix("kafka");
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);
        String topics = props.getProperty("topics");
        if (topics == null || topics.trim().equals(""))
            throw new RuntimeException("非法的 kafka topic " + topics);

        String[] topicList = topics.trim().split(",");
        consumer.subscribe(Arrays.asList(topicList));

        for (; ; ) {
            ConsumerRecords<String, String> msgList = consumer.poll(Duration.ofSeconds(30));
            if (null != msgList && msgList.count() > 0) {
                for (ConsumerRecord<String, String> record : msgList) {
                    Processor pro = new Processor();
                    pro.processOneRecord(record);
                }
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.error("InterruptedException e  {}", e);
                    throw new RuntimeException("InterruptedException e " + e);
                }
            }
        }
    }

}
