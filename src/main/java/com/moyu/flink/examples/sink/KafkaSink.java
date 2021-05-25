package com.moyu.flink.examples.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 *      数据写入kafka测试
 *              ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic kafka_sink --replication-factor 1 --partitions 1
 */
public class KafkaSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        Properties kafkaConf = new Properties();
        kafkaConf.setProperty("bootstrap.servers", "localhost:9092");

        /**
         *      写入数据到kafka使用FlinkKafkaProducer
         */
        FlinkKafkaProducer<String> kafkaSink =
                new FlinkKafkaProducer<>("kafka_sink", new SimpleStringSchema(), kafkaConf);

        // 添加输出目标
        socketSource.print();
        socketSource.addSink(kafkaSink);

        env.execute("Kafka Sink Test Job");
    }
}
