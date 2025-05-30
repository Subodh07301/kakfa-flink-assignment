# Kafka Flink Assignment

## Description
Flink streaming job that reads from Kafka topic `topic1`, validates JSON messages, and routes them accordingly:
- Valid messages are written to `topic2` and local disk in CSV format
- Invalid messages are routed to a dead-letter queue topic `topic1.DLQ`

## Technologies
- Apache Flink
- Apache Kafka
- JSON Schema Validation
- Docker Compose
- Java 11
- Maven

## Setup
### Prerequisites
- Java 11+
- Maven
- Docker + Docker Compose

### Running Services
```bash
docker-compose up -d
```
This starts Kafka, Zookeeper, and Flink.

### Creating Topics
```bash
docker exec -it kafka kafka-topics --create --topic topic1 --bootstrap-server localhost:9092 --partitions 25 --replication-factor 1 && \
docker exec -it kafka kafka-topics --create --topic topic2 --bootstrap-server localhost:9092 --partitions 25 --replication-factor 1 && \
docker exec -it kafka kafka-topics --create --topic topic1.DLQ --bootstrap-server localhost:9092 --partitions 25 --replication-factor 1
```

### Building the Project
```bash
mvn clean package
```

### Running the Flink Job
```bash
 flink run target/KafkaFlinkAssignment-1.0-SNAPSHOT.jar
```

### Output
Valid data will be written to:
- Kafka topic: `topic2`
- Local file: `output/valid/`

Invalid data will be sent to:
- Kafka topic: `topic1.DLQ`

### Running Tests
```bash
mvn test