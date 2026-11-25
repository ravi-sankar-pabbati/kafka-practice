# Kafka Practice

A local single-node Apache Kafka setup (KRaft mode, no ZooKeeper) with Kafka UI, for learning and experimenting.

## What's included

| Service  | Image                       | Port | Purpose                                  |
|----------|-----------------------------|------|------------------------------------------|
| kafka    | `apache/kafka:latest`       | 9092 | Broker + controller (apps on your Mac)   |
| kafka-ui | `provectuslabs/kafka-ui`    | 8080 | Web UI to browse topics and consumer groups |

Listeners:

- `localhost:9092`: for apps running on your host machine
- `kafka:19092`: for other containers on the same Docker network (Kafka UI uses this)
- `9093`: internal KRaft controller

Defaults for practice: new topics get 3 partitions, replication factor is 1, and topic auto-creation is on. Data persists in the `kafka-data` Docker volume.

## Prerequisites

- Docker and Docker Compose

## Start and stop

```bash
docker compose up -d        # start
docker compose ps           # check status (kafka should be "healthy")
docker compose logs -f kafka
docker compose down         # stop, keep data
docker compose down -v      # stop and delete all data
```

Kafka UI: http://localhost:8080

## Basic commands

All commands run inside the broker container.

```bash
# Create a topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demo --partitions 3

# List and describe topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic demo

# Produce (type lines, Ctrl+C to exit)
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo

# Consume
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo --from-beginning
```

## Practice exercises

### 1. Keys and partitions

Messages with the same key always go to the same partition.

```bash
# Producer with keys (type e.g. user1:hello)
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo \
  --property parse.key=true --property key.separator=:

# Consumer showing keys and partitions
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo \
  --from-beginning --property print.key=true --property print.partition=true
```

### 2. Consumer groups

A consumer group is one logical consumer application; each consumer in it is one running instance. Kafka assigns each partition to exactly one consumer in the group, and tracks the group's offsets.

Run this in two terminals and watch the partitions split between them:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo --group mygroup
```

Stop one and watch the rebalance. Inspect the group (it must exist, so start a consumer in it first):

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group mygroup
```

Without `--group`, the console consumer uses a random throwaway group, so it has no saved offsets and won't resume between runs. Different groups each receive every message.

### 3. Offsets and replay

Stop all consumers in the group first, then:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group mygroup --topic demo --reset-offsets --to-earliest --execute
```

## Troubleshooting

- **`GroupIdNotFoundException: Group mygroup not found`**: no consumer has joined that group yet. Start a consumer with `--group mygroup` first, then run `--describe`.
- **Kafka UI shows no cluster**: wait for the kafka container to report `healthy` (`docker compose ps`).
- **Connection refused from your app**: use `localhost:9092` from the host and `kafka:19092` from inside Docker.

## Java demo: key-based partitioning and consumer groups

A runnable Maven project in [java-demo/](java-demo/) with a topic setup tool, a keyed producer and a group-aware consumer. It shows:

- the same key always lands in the same partition
- consumers in one group split the partitions, and rebalance when one joins or leaves
- different groups each receive every message

Quick start (needs Java 17+ and Maven, e.g. `brew install maven`):

```bash
cd java-demo
mvn -q exec:java -Dexec.mainClass=com.example.kafka.TopicSetup
mvn -q exec:java -Dexec.mainClass=com.example.kafka.KeyedProducer
mvn -q exec:java -Dexec.mainClass=com.example.kafka.GroupConsumer -Dexec.args="group-A consumer-1"
```

See [java-demo/README.md](java-demo/README.md) for the full walkthrough.

## Project structure

```
kafka-practice/
  docker-compose.yml   # Kafka (KRaft) + Kafka UI
  README.md            # this file
  java-demo/           # Java Maven demo project
```

## Next steps

- Manual offset commits and at-least-once delivery
- Producer reliability: `acks`, idempotence, retries
- A custom partitioner, and hot-key effects on partition load
- A multi-broker cluster with replication
- Python or Node clients
