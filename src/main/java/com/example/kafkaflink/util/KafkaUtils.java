package com.example.kafkaflink.util;

import java.util.Properties;

public class KafkaUtils {
    public static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-consumer-group");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "earliest");
        return props;
    }
}
