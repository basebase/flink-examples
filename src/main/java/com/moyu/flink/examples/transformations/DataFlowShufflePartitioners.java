package com.moyu.flink.examples.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 *      随机交换策略
 */
public class DataFlowShufflePartitioners {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 8888);

        /***
         *      replaceMap算子通过shuffle传输数据到upStrMap算子
         *      shuffle:
         *          1. DataStream通过调用shuffle来实现随机数据交换策略
         *          2. shuffle会随机选择一条channel发送数据, 但是Random生成的随机数符合均匀分布, 故能近似保持平均
         */
        socketSource.map(value -> value.replaceAll(",", "")).name("replaceMap").setParallelism(2)
                .shuffle()
                .map(value -> value.toUpperCase()).name("upStrMap").setParallelism(3)
                .map(value -> value.toLowerCase()).name("lowMap").setParallelism(2).shuffle()
                .map(value -> value.getBytes().length).name("strSize").setParallelism(1).print();

        env.execute("DataFlow Shuffle Partition Test Job");
    }
}
