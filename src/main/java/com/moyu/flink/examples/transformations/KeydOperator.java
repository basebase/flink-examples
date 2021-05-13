package com.moyu.flink.examples.transformations;

import com.moyu.flink.examples.model.Student;
import com.moyu.flink.examples.source.FileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * keyBy算子使用
 */
public class KeydOperator {

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

        // 2. 想要对算子做一些聚合计算, 需要进行keyBy分组
        // 2.1 数据类型从DataStream转为KeydStream, 不过本质还是一个DataStream
        KeyedStream<Student, Tuple> studentKeyedStream = mapOperator.keyBy("id");

        /**
         *     max的使用:
         *      相同key数据如果当前数据比上一条数据的score值大, 则替换score值, 其余数据不替换, 即: 只更新max中的字段值, 其它数据则以第一条进入的数据为标准
         *
         *     maxBy使用:
         *      相同key数据如果当前数据比上一条数据的score值大, 则使用当前一整条数据替换, 即: 当前数据是最大score全部内容数据
         */
        studentKeyedStream.max("score").print();
        studentKeyedStream.maxBy("score").print();

        env.execute("Keyd Operator Test Job");
    }
}
