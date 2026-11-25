package com.example.kafka;

/** Shared constants. Override the broker with the KAFKA_BOOTSTRAP env var if needed. */
public final class Config {

    public static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

    public static final String TOPIC = "orders";
    public static final int PARTITIONS = 3;
    public static final short REPLICATION_FACTOR = 1;

    private Config() {
    }
}
