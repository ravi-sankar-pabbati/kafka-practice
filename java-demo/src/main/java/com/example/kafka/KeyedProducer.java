package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Sends keyed messages and prints which partition each key lands in.
 * Usage: KeyedProducer [messageCount]   (default 30)
 */
public class KeyedProducer {

    private static final String[] KEYS = {"user-1", "user-2", "user-3", "user-4", "user-5"};

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 30;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // key -> set of partitions it was written to (should always be exactly one)
        Map<String, Set<Integer>> keyToPartitions = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(count);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String key = KEYS[i % KEYS.length];
                String value = "order-" + i + " for " + key;

                producer.send(new ProducerRecord<>(Config.TOPIC, key, value), (RecordMetadata md, Exception ex) -> {
                    if (ex != null) {
                        System.err.println("Send failed: " + ex);
                    } else {
                        keyToPartitions.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(md.partition());
                        System.out.printf("key=%-7s -> partition=%d offset=%d%n", key, md.partition(), md.offset());
                    }
                    done.countDown();
                });
            }
            producer.flush();
            done.await();
        }

        System.out.println();
        System.out.println("Summary (each key should map to exactly one partition):");
        new TreeMap<>(keyToPartitions).forEach((key, partitions) ->
                System.out.printf("  %-7s -> partitions %s%n", key, new TreeSet<>(partitions)));
    }
}
