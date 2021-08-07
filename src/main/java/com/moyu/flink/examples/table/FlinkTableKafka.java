package com.moyu.flink.examples.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.types.Row;

/**
 *  基于Kafka数据通道测试
 */
public class FlinkTableKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        String createKafkaSourceTable = "CREATE TABLE student_kafka_source (\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  score DOUBLE\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'student_source',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'student_source_group',\n" +
                " 'property-version' = 'universal',\n" +
                " 'format' = 'json',\n" +
                " 'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'false'\n" +
                ")";
        tabEnv.executeSql(createKafkaSourceTable);

        String createKafkaSinkTable = "CREATE TABLE student_kafka_sink (\n" +
                "  name STRING,\n" +
                "  score DOUBLE\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'student_sink',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'student_sink_group',\n" +
                " 'property-version' = 'universal',\n" +
                " 'format' = 'json',\n" +
                " 'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'false'\n" +
                ")";
        tabEnv.executeSql(createKafkaSinkTable);

        String sql = "select name, score from student_kafka_source";
        Table studentQueryResult = tabEnv.sqlQuery(sql);
        studentQueryResult.printSchema();
        tabEnv.toAppendStream(studentQueryResult, Row.class).print();

//        studentQueryResult.executeInsert("student_kafka_sink");

        tabEnv.executeSql("insert into student_kafka_sink select name, score from student_kafka_source");

        env.execute("FlinkTableKafka Test Job");
    }
}
