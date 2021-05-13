package com.moyu.flink.examples.transformations;

import com.moyu.flink.examples.model.Student;
import com.moyu.flink.examples.source.FileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 *  Reduce算子使用
 */
public class ReduceOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        String inPath = FileSource.class.getClassLoader().getResource("student").getPath();
        // 读取文件数据作为数据源
        DataStreamSource<String> fileSource = env.readTextFile(inPath);

        // 1. 处理数据, 转换为Student对象
        DataStream<Student> mapOperator = fileSource.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] fileds = value.split(",");
                return new Student(Integer.parseInt(fileds[0]), fileds[1], Double.parseDouble(fileds[2]));
            }
        });

        mapOperator.keyBy("id")
                .reduce(new ReduceFunction<Student>() {
                    // 不同key下num是不共享的
                    private int num = 0;
                    @Override
                    public Student reduce(Student value1, Student value2) throws Exception {
                        num += 1;
                        System.out.println("value1: " + value1 + " value2: " + value2);
                        System.out.println("key(" + value1.getId() + ")调用次数: " + num);
                        // 使用最新数据的名称替代旧的名称, 并且累加所有分数
                        return new Student(value1.getId(), value2.getName(), value2.getScore() + value1.getScore());
                    }
                }).print();

        env.execute("Reduce Operator Test Job");
    }
}
