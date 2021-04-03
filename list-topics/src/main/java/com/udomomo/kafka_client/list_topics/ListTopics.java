package com.udomomo.kafka_client.list_topics;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Set;

public class ListTopics {
  public static void main(String[] args) throws Exception {
    Logger logger = LoggerFactory.getLogger(ListTopics.class);

    Properties properties = new Properties();
    try (FileInputStream fis = new FileInputStream(System.getenv("KAFKA_CONFIG_FILEPATH"))) {
      properties.load(fis);
    }

    AdminClient client = KafkaAdminClient.create(properties);
    ListTopicsResult result = client.listTopics();
    Set<String> names = result.names().get();
    logger.info(names.toString());
  }
}
