package com.moyu.flink.examples.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 *          全局数据交换策略
 */
public class DataFlowGlobalPartitioners {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        /***
         *      global:
         *          1. DataStream通过调用global()方法实现全局策略
         *          2. 上游算子所有数据都发送到下游算子第一个子任务中
         */
        socketSource.map(value -> value.replaceAll(",", "")).name("replaceMap").setParallelism(2)
                .global()
                .map(value -> value.toUpperCase()).name("upStrMap").setParallelism(4)
                .global()
                .map(value -> value.toLowerCase()).name("lowMap").setParallelism(2)
                .map(value -> value.getBytes().length).name("strSize").setParallelism(1).print();


        env.execute("DataFlow Global Partition Test Job");
    }
}
