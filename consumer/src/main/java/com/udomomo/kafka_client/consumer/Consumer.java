package com.udomomo.kafka_client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {
  public static void main(String[]args) throws Exception {
    Logger logger = LoggerFactory.getLogger(Consumer.class);

    Properties properties = new Properties();
    try (FileInputStream fis = new FileInputStream(System.getenv("KAFKA_CONFIG_FILEPATH"))) {
    properties.load(fis);
    }

    final String topic = "test";
    List<String> topicList = new ArrayList<>();
    topicList.add(topic);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(topicList);

    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
    for (ConsumerRecord<String, String> record : records) {
      logger.info(record.key() + "," + record.value());
    }
    consumer.close();
  }

}
