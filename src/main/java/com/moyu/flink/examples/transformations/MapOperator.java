package com.moyu.flink.examples.transformations;

import com.moyu.flink.examples.source.FileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * map算子使用
 */
public class MapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        String inPath = FileSource.class.getClassLoader().getResource("student").getPath();
        // 读取文件数据作为数据源
        DataStreamSource<String> fileSource = env.readTextFile(inPath);

        /***
         *      MapFunction中的泛型T和O分别代表为:
         *          T: 调用map算子的泛型类型，也就是String，可以看成是我们的输入类型
         *          O: 通过map算子转换后的类型，也就是Integer，可以看成是输出类型
         *
         */
        SingleOutputStreamOperator<Integer> mapOperator = fileSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        mapOperator.print();

        env.execute("Map Operator Test Job");
    }
}
