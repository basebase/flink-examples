package com.moyu.flink.examples.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 *      轮询交换策略
 */
public class DataFlowRebalancePartitioners {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 8888);


        /***
         *      replaceMap算子通过rebalance传输数据到upStrMap算子
         *      rebalance:
         *
         *          1. DataStream通过调用rebalance()方法来实现轮询交换策略
         *
         *          2. 利用ThreadLocalRandom.current().nextInt()随机函数生成一个随机数, 已选择第一个要发送的下游算子子任务
         *             并通过轮询的方式从该实例开始循环输出, 保证下游数据负载均衡
         *
         *
         */
        socketSource.map(value -> value.replaceAll(",", "")).name("replaceMap").setParallelism(2)
                .rebalance()
                .map(value -> value.toUpperCase()).name("upStrMap").setParallelism(3)
                .map(value -> value.toLowerCase()).name("lowMap").setParallelism(2).rebalance()
                .map(value -> value.getBytes().length).name("strSize").setParallelism(1).print();


        env.execute("DataFlow Rebalance Partition Test Job");
    }
}
