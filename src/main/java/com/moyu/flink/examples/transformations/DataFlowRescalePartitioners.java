package com.moyu.flink.examples.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *      重调交换策略
 */
public class DataFlowRescalePartitioners {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        /***
         *      rescale:
         *          1. DataStream通过调用rescale()方法实现重调交换策略
         *
         *          2. rescale也是轮询的方式对数据进行分发, 它和rebalance很像但是有一点不同
         *             rebalance将数据轮询发送给下游算子每个实例, 而rescale发送数据只会选择部分实例发送
         */
        socketSource.map(value -> value.replaceAll(",", "")).name("replaceMap").setParallelism(2)
                .rescale()
                .map(value -> value.toUpperCase()).name("upStrMap").setParallelism(4)
                .rescale()
                .map(value -> value.toLowerCase()).name("lowMap").setParallelism(2)
                .map(value -> value.getBytes().length).name("strSize").setParallelism(1).print();


        env.execute("DataFlow Rescale Partition Test Job");
    }
}
