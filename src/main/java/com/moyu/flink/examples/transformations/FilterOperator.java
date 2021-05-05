package com.moyu.flink.examples.transformations;

import com.moyu.flink.examples.source.FileSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * filter算子使用
 */
public class FilterOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        String inPath = FileSource.class.getClassLoader().getResource("student").getPath();
        // 读取文件数据作为数据源
        DataStreamSource<String> fileSource = env.readTextFile(inPath);

        SingleOutputStreamOperator<String> filterOperator = fileSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.length() >= 10;
            }
        });

        filterOperator.print();


        env.execute("Filter Operator Test Job");
    }
}
