package com.moyu.flink.examples.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *      广播数据交换策略
 */
public class DataFlowBroadcastPartitioners {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        /***
         *      broadcast:
         *          1. DataStream通过调用broadcast()方法实现广播策略
         *
         *          2. 上游算子数据复制并发送到下游算子所有子任务中
         */
        socketSource.map(value -> value.replaceAll(",", "")).name("replaceMap").setParallelism(2)
                .broadcast()
                .map(value -> value.toUpperCase()).name("upStrMap").setParallelism(4)
                .broadcast()
                .map(value -> value.toLowerCase()).name("lowMap").setParallelism(2)
                .map(value -> value.getBytes().length).name("strSize").setParallelism(1).print();


        env.execute("DataFlow Rescale Partition Test Job");
    }
}
