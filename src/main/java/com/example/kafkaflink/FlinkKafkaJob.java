package com.example.kafkaflink;

import com.example.kafkaflink.util.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FlinkKafkaJob {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = KafkaUtils.getKafkaProperties();
        JsonValidator validator = new JsonValidator();
        OutputTag<String> invalidTag = new OutputTag<>("invalid") {};

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), props);
        DataStream<String> stream = env.addSource(consumer);

        SingleOutputStreamOperator<String> validStream = stream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) {
                if (validator.isValid(value)) {
                    out.collect(value);
                } else {
                    ctx.output(invalidTag, value);
                }
            }
        });

        // Kafka sink for valid messages
        validStream.addSink(new FlinkKafkaProducer<>("topic2", new SimpleStringSchema(), props));

        // File sink for valid messages
        StreamingFileSink<String> fileSink = StreamingFileSink
                .forRowFormat(new Path("output/valid"), new SimpleStringEncoder<String>())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("valid-data")
                        .withPartSuffix(".txt")
                        .build())
                .build();

        validStream.addSink(fileSink);

        // Kafka sink for invalid messages
        DataStream<String> invalidStream = validStream.getSideOutput(invalidTag);
        invalidStream.addSink(new FlinkKafkaProducer<>("topic1.DLQ", new SimpleStringSchema(), props));

        env.execute("Kafka Flink Validation Job");
    }
}