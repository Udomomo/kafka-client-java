package com.udomomo.kafka_client.producer;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

public class Producer {
  public static void main(String[] args) throws Exception {
    Logger logger = LoggerFactory.getLogger(Producer.class);

    Properties properties = new Properties();
    try (FileInputStream fis = new FileInputStream(System.getenv("KAFKA_CONFIG_FILEPATH"))) {
      properties.load(fis);
    }

    final String topic = "test";
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    for (int i=0; i<10; i++) {
      final String key = "key" + i;
      final String value = "value" + i;
      producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
        if (metadata != null) {
          logger.info(String.format(
            "Success partition:%d, offset:%d", metadata.partition(), metadata.offset()
          ));
        } else {
          logger.error(String.format(
            "Failed:%s", exception.getMessage()
          ));
        }
      });
    }
    producer.flush();
    producer.close();
  }
}
