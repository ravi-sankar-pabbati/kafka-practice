package com.example.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/** Creates the demo topic with 3 partitions. Safe to run repeatedly. */
public class TopicSetup {

    public static void main(String[] args) throws Exception {
        Map<String, Object> props = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);

        try (Admin admin = Admin.create(props)) {
            NewTopic topic = new NewTopic(Config.TOPIC, Config.PARTITIONS, Config.REPLICATION_FACTOR);
            try {
                admin.createTopics(List.of(topic)).all().get();
                System.out.printf("Created topic '%s' with %d partitions%n", Config.TOPIC, Config.PARTITIONS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.printf("Topic '%s' already exists, nothing to do%n", Config.TOPIC);
                } else {
                    throw e;
                }
            }
        }
    }
}
