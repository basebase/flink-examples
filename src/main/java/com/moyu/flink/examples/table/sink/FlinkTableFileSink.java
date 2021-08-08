package com.moyu.flink.examples.table.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 *      Flink Table数据写入到文件中
 *
 */
public class FlinkTableFileSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

        // 创建一张数据源表
        String inPath = FlinkTableFileSink.class.getClassLoader().getResource("student").getPath();
        String sourceTable = "create temporary table source_table (\n" +
                "    id int,\n" +
                "    name string,\n" +
                "    score double\n" +
                ") with (\n" +
                "    'connector'='filesystem',\n" +
                "    'path'= '"+ inPath +"',\n" +
                "    'format'='csv'\n" +
                ")";
        // 创建一张表
        tabEnv.executeSql(sourceTable);


        // 查询数据
        String studentScoreQuery = "select name, score from source_table";
        Table studentScoreResult = tabEnv.sqlQuery(studentScoreQuery);

        // 聚合数据
        String studentCountQuery = "select count(distinct id) from source_table";
        Table studentCountResult = tabEnv.sqlQuery(studentCountQuery);


        // 目标表, 非聚合数据
        String studentScoreSinkPath = ""; //FlinkTableFileSink.class.getClassLoader().getResource("student_score_path").getPath();
        studentScoreSinkPath = "student_score_path";
        String studentScoreSinkTable = "create table student_score_sink (\n" +
                "    name string,\n" +
                "    score double\n" +
                ") with (\n" +
                "    'connector'='filesystem',\n" +
                "    'path'= '"+ studentScoreSinkPath +"',\n" +
                "    'format'='csv'\n" +
                ")";

        tabEnv.executeSql(studentScoreSinkTable);
        tabEnv.executeSql("insert into student_score_sink select name, score from source_table").print();

        // 目标表, 聚合数据
        String studentCountSinkPath = ""; //FlinkTableFileSink.class.getClassLoader().getResource("student_count_path").getPath();
        studentCountSinkPath = "student_count_path";
        String studentCountSinkTable = "create temporary table student_score_sink ( \n" +
                "cnt bigint\n" +
                ") with(\n" +
                "    'connector'='filesystem',\n" +
                "    'path'= '"+ studentCountSinkPath +"',\n" +
                "    'format'='csv'\n" +
                ")";
        tabEnv.executeSql(studentCountSinkTable);
        // filesystem不支持聚合类写入数据, 由于无法更新数据
//        tabEnv.executeSql("insert into student_score_sink select count(distinct id) from source_table");

        // 加上这个就会出现异常No operators defined in streaming topology. Cannot execute.
//        env.execute("Flink Table File Sink Test Job");
    }
}
