package com.moyu.flink.examples.transformations;

import com.moyu.flink.examples.source.FileSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flatMap算子使用
 */
public class FlatMapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();


        String inPath = FileSource.class.getClassLoader().getResource("student").getPath();
        // 读取文件数据作为数据源
        DataStreamSource<String> fileSource = env.readTextFile(inPath);

        /***
         * 和map方法类似，只不过flatMap可以将一个对象包装成更多的对象发送出去
         */
        SingleOutputStreamOperator<String> flatMapOperator = fileSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String data, Collector<String> collector) throws Exception {
                String[] values = data.split(",");
                for (String value : values) {
                    collector.collect(value);
                }
            }
        });

        flatMapOperator.print();

        env.execute("FlatMap Operator Test Job");
    }
}
