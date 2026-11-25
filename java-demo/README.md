# Kafka key-based partitioning and consumer groups (Java demo)

Demonstrates two ideas against the local broker from `../docker-compose.yml`:

1. Messages with the same key always go to the same partition.
2. Consumers in one group split the partitions; different groups each get every message.

## Requirements

- Java 17+, Maven 3.8+
- Kafka running: `docker compose up -d` in the parent folder (broker on `localhost:9092`)

Install Maven on macOS with `brew install maven`. Set `KAFKA_BOOTSTRAP` to use a different broker address.

Arguments to the demo classes go through `-Dexec.args="..."`. A bare argument after the Maven command is treated as a Maven phase and fails.

## Setup

```bash
cd java-demo
mvn -q compile
mvn -q exec:java -Dexec.mainClass=com.example.kafka.TopicSetup
```

This creates topic `orders` with 3 partitions (safe to rerun).

## Demo 1: same key, same partition

```bash
mvn -q exec:java -Dexec.mainClass=com.example.kafka.KeyedProducer
```

Sends 30 messages across keys `user-1` to `user-5`. The summary at the end shows each key mapped to exactly one partition. Run it again: the key-to-partition mapping stays the same.

To send a different number of messages: `-Dexec.args="100"`.

Example summary (the exact partitions can differ, but are stable per key):

```
user-1  -> partitions [2]
user-2  -> partitions [2]
user-3  -> partitions [2]
user-4  -> partitions [1]
user-5  -> partitions [1]
```

Several keys can share a partition, and some partitions can stay empty. With only 5 keys, hashing doesn't spread them evenly.

## Demo 2: consumers in one group split partitions

Open two terminals and run, one in each:

```bash
mvn -q exec:java -Dexec.mainClass=com.example.kafka.GroupConsumer -Dexec.args="group-A consumer-1"
mvn -q exec:java -Dexec.mainClass=com.example.kafka.GroupConsumer -Dexec.args="group-A consumer-2"
```

Watch the `ASSIGNED` lines: the 3 partitions are divided between them (e.g. 2 and 1). Then, in a third terminal, run the producer again. Each message appears in only one consumer, and all messages for a given key appear in the same consumer.

Press Ctrl+C on one consumer. The other gets a new `ASSIGNED` event covering all 3 partitions (the rebalance).

A third consumer in `group-A` takes over one partition. A fourth sits idle, because there are only 3 partitions.

## Demo 3: different groups each get everything

While `group-A` consumers are running, start one in another group:

```bash
mvn -q exec:java -Dexec.mainClass=com.example.kafka.GroupConsumer -Dexec.args="group-B consumer-3"
```

It reads all messages from the beginning (`auto.offset.reset=earliest`), independent of `group-A`. New messages go to both groups.

## Notes

- A new group starts from the beginning of the topic (`auto.offset.reset=earliest`), so the first consumer in a group reads the whole backlog before it sees new messages. Wait for that before judging how work is split.
- The `SLF4J: Failed to load class ... StaticLoggerBinder` warning is harmless. It only affects Kafka's internal logging, not the demo output.
- Stop consumers with Ctrl+C in their terminal. A shutdown hook closes them cleanly, so the group rebalances promptly.

## Inspect the group

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group-A
```

Shows partition-to-consumer assignment, committed offsets and lag. The group must have joined at least once.

## Reset to replay

Stop the group's consumers, then:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group group-A --topic orders --reset-offsets --to-earliest --execute
```

## Files

| File | Purpose |
|------|---------|
| `Config.java` | Broker address, topic name and partition count |
| `TopicSetup.java` | Creates the `orders` topic via `AdminClient` |
| `KeyedProducer.java` | Sends keyed messages, prints key, partition and offset |
| `GroupConsumer.java` | Joins a group, prints assignments (rebalance listener) and records |
