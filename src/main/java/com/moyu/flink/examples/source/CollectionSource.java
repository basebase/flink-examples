package com.moyu.flink.examples.source;

import com.moyu.flink.examples.model.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 基于集合的数据源
 */
public class CollectionSource {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建一个学生集合数据源
        DataStreamSource<Student> collectionSource = env.fromCollection(Arrays.asList(
                new Student(1, "A", 111.1),
                new Student(2, "B", 222.2),
                new Student(3, "C", 333.3),
                new Student(4, "D", 444.4),
                new Student(5, "E", 555.5)
        ));

        // 3. 创建一个数字集合数据源
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3);
        // 抛出异常
//        DataStreamSource<? extends Serializable> dataStreamSource = env.fromElements(1, 2, "3");


        // 4. 打印输出
        collectionSource.print("collection source");
        integerDataStreamSource.print("int source");


        // 5. 执行
        env.execute("Collection Source Test Job");
    }
}
