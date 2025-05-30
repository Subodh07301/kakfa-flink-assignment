package com.example.kafkaflink;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.*;

public class IntegrationTest {

    @Test
    public void testKafkaStartup() {
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"))) {
            kafka.start();
            assertTrue(kafka.isRunning());
            assertNotNull(kafka.getBootstrapServers());
        }
    }

    // You can extend this with Flink job tests using embedded Kafka producers/consumers
}