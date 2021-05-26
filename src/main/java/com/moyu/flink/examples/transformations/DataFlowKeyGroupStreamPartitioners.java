package com.moyu.flink.examples.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 *      键值策略
 */
public class DataFlowKeyGroupStreamPartitioners {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        /***
         *      使用keyBy进行分组
         */

        socketSource.map(value -> value.replaceAll(",", "")).name("replaceMap").setParallelism(2)
                .keyBy(value -> value)
                .map(value -> value.toUpperCase()).name("upStrMap").setParallelism(4)
                .keyBy(value -> value)
                .map(value -> value.toLowerCase()).name("lowMap").setParallelism(2)
                .map(value -> value.getBytes().length).name("strSize").setParallelism(1).print();

        env.execute("DataFlow KeyGroup Partition Test Job");
    }
}
