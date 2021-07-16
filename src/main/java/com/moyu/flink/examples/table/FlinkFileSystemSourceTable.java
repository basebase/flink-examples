package com.moyu.flink.examples.table;

import com.moyu.flink.examples.source.FileSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/****
 *
 *
 *      Flink读取文件数据
 */

public class FlinkFileSystemSourceTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String inPath = FlinkFileSystemSourceTable.class.getClassLoader().getResource("student").getPath();
        String sourceTable = "create temporary table source_table (\n" +
                "    id int,\n" +
                "    name string,\n" +
                "    score double\n" +
                ") with (\n" +
                "    'connector'='filesystem',\n" +
                "    'path'= '"+ inPath +"',\n" +
                "    'format'='csv'\n" +
                ")";

        tableEnv.executeSql(sourceTable);

        String querySQL = "select * from source_table";
        Table result = tableEnv.sqlQuery(querySQL);

        result.printSchema();
        tableEnv.toAppendStream(result, Row.class).print("result");


        env.execute("FlinkFileSystemSourceTable Test Job");
    }
}
