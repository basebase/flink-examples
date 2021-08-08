package com.moyu.flink.examples.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 *     基于Kafka数据源写入数据到mysql
 */
public class FlinkTableMysqlUpsert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

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


        String createKafkaSinkTable = "CREATE TABLE student_mysql_upsert_sink (\n" +
                "  name STRING,\n" +
                "  score DOUBLE,\n" +
                "  num BIGINT,\n" +
                "  PRIMARY KEY (name) NOT ENFORCED" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&autoReconnect=true',\n" +
                " 'table-name' = 'student_mysql_upsert_sink',\n" +
                " 'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                " 'username' = 'root',\n" +
                " 'password' = ''\n" +
                ")";
        tabEnv.executeSql(createKafkaSinkTable);

        String sql = "select name, sum(score) as score, count(name) as num from student_kafka_source group by name";
        Table queryTableResult = tabEnv.sqlQuery(sql);
        queryTableResult.executeInsert("student_mysql_upsert_sink");

//        env.execute("");
    }
}
