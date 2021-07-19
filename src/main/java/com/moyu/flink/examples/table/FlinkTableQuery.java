package com.moyu.flink.examples.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/***
 *
 *      Flink Table API & SQL查询
 *
 */
public class FlinkTableQuery {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

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
        tabEnv.executeSql(sourceTable);
        Table studentTable = tabEnv.from("source_table");

        /*************************************select使用******************************************/
        studentTable.select($("name").as("n1"), $("score")).printSchema();
        studentTable.select($("*")).printSchema();


        /****************************************as使用*******************************************/
        studentTable.as("a1", "a2", "a3").printSchema();


        /****************************************where使用****************************************/
        Table where = studentTable.where(
                and(
                        $("id").isGreaterOrEqual(3),
                        $("score").isGreaterOrEqual(60),
                        or( $("name").like("A%"), $("name").like("E%") )
                )
        );
        tabEnv.toAppendStream(where, Row.class).print("where");


        /****************************************groupBy使用**************************************/
        Table groupBy = studentTable.groupBy($("id"))
                .select($("id").count(), $("score").sum());
        /***
         *  注意: 这里不能使用toAppendStream方式输出, 而是使用toRetractStream
         *       因为聚合操作会对值进行更新变化, 而不是来一条输出一条的无状态数据
         */
        tabEnv.toRetractStream(groupBy, Row.class).print("groupBy");


        env.execute("FlinkTableQuery Test Job");
    }
}
