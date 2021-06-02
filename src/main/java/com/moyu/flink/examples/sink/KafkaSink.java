package com.moyu.flink.examples.sink;

import com.moyu.flink.examples.model.Student;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
//        FlinkKafkaProducer<String> kafkaSink =
//                new FlinkKafkaProducer<>("kafka_sink", new SimpleStringSchema(), kafkaConf);
//
//        SingleOutputStreamOperator<String> strStream = socketSource.filter(value -> value != null && value.length() > 0).map(value -> {
//            return value.replaceAll(",", "").toUpperCase();
//        });
//
//        strStream.addSink(kafkaSink);


        /***
         *      将一个java bean序列化写入到kafka中, 但其实可以重写tostring方法利用SimpleStringSchema来实现比较方便
         */
        FlinkKafkaProducer<Student> studentKafkaSink =
                new FlinkKafkaProducer<>("kafka_sink", new StudentSerializationSchema(), kafkaConf);

        DataStream<Student> studentStream = socketSource.filter(value -> value != null && value.length() > 0).map(value -> {
            String[] fields = value.split(",");
            return new Student(Integer.parseInt(fields[0]), fields[1], Double.parseDouble(fields[2]));
        }).returns(Types.POJO(Student.class));


        studentStream.print();

        // 添加输出目标
        studentStream.addSink(studentKafkaSink);

        env.execute("Kafka Sink Test Job");
    }

    private static class StudentSerializationSchema implements SerializationSchema<Student> {
        ObjectMapper mapper;

        @Override
        public byte[] serialize(Student element) {
            byte[] b = null;
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                b= mapper.writeValueAsBytes(element);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return b;
        }
    }
}
