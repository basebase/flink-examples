package com.moyu.flink.examples.table;

import com.moyu.flink.examples.model.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;


/***
 *      Flink Table & SQL 快速入门例子
 */

public class FlinkTableApiQuickStack {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketStream =
                env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Student> studentStream =
                socketStream.filter(line -> line != null && line.length() > 0 && line.split(",").length == 3)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Student(Integer.parseInt(fields[0]), fields[1], Double.parseDouble(fields[2]));
                });


        // 1. 创建TableEvn环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        // 2. 将stream转为Table
        Table studentTable = tableEnv.fromDataStream(studentStream);

        // 3. 我们需要输出学生姓名和分数, 但是分数需要大于60分
        Table studentTableResult = studentTable.select($("name"), $("score"))
                .where("score >= 60");



        /////////////////////////////////////////////////////////上面是Table API我们还可以使用SQL API//////////////////////////////////////////////////////////////

        // 1. 需要使用SQL则先注册一张表在里面, 可以使用DataStream也可以使用Table
        tableEnv.createTemporaryView("student", studentTable);

        // 2. sql语句
        String sql = "select count(distinct name) as num from student where score >= 60";

        // 3. 执行sql, 返回table
        Table studentSQLTableResult = tableEnv.sqlQuery(sql);

        // 4. 没有直接输出的api, 这里打印的是表的结构schema
        studentSQLTableResult.printSchema();

        // 5. sql & table两种API结果进行输出
        tableEnv.toAppendStream(studentTableResult, Row.class).print("studentTable");
//        tableEnv.toAppendStream(studentSQLTableResult, Row.class).print("studentSQL");
        tableEnv.toRetractStream(studentSQLTableResult, Row.class).print("studentSQL");


        env.execute("FlinkTableApiQuickStack Test Job");

    }
}