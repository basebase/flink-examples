package com.moyu.flink.examples.transformations;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *      转发策略
 */
public class DataFlowForwardPartitioners {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        /***
         *
         *      Exception in thread "main" java.lang.UnsupportedOperationException: Forward partitioning does not allow change of parallelism. Upstream operation: replaceMap-2 parallelism: 2, downstream operation: upStrMap-4 parallelism: 4 You must use another partitioning strategy, such as broadcast, rebalance, shuffle or global.
         *
         *      使用forward分区要注意, 如果上下游算子并行度不一致的话则抛出上面异常, 让其使用其它分区
         *
         *          注意: replaceMap算子和upStrMap算以及lowMap算子并行度必须是一致的
         */

        socketSource.map(value -> value.replaceAll(",", "")).name("replaceMap").setParallelism(2)
                .forward()
                .map(value -> value.toUpperCase()).name("upStrMap").setParallelism(2)
                .forward()
                .map(value -> value.toLowerCase()).name("lowMap").setParallelism(2).disableChaining()
                .map(value -> value.getBytes().length).name("strSize").setParallelism(2).print();

        env.execute("DataFlow Forward Partition Test Job");
    }
}
