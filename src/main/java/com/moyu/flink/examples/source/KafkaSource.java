package com.moyu.flink.examples.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 *  基于kafka的数据源
 */
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加kafka配置
        Properties kafkaPorps = new Properties();
        // kafka集群
        kafkaPorps.setProperty("bootstrap.servers", "localhost:9092");
        // kafka消费组
        kafkaPorps.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> kafkaConsumer =
                // 参数1: kafka topic
                // 参数2: 反序列化kafka数据类型
                // 参数3: kafka参数
                new FlinkKafkaConsumer<>("test_topic", new SimpleStringSchema(), kafkaPorps);

        // 通过addSource添加数据源
        DataStreamSource<String> kafkaSource = env.addSource(kafkaConsumer);

        kafkaSource.print("kafka source");
        env.execute("Kafka Source Test Job");
    }
}
