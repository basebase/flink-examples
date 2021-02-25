package com.moyu.flink.examples.wordcount.streaming;

import com.moyu.flink.examples.wordcount.batch.WordCountBatch;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreaming {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);      // 设置并行度

//        DataStreamSource<String> stringDataStreamSource = env.readTextFile("");

        /***
         *      2. 尝试读取文本数据, 返回是一个DataStreamSource类型, 但是深入DataStreamSource类发现其继承SingleOutputStreamOperator
         *         而SingleOutputStreamOperator继承DataStream, 本质是操纵DataStream数据集, 所以flink流处理API被称为DataStream API
         *
         *         一般DataStream处理无限的数据集
          */
//        String inPath = WordCountBatch.class.getClassLoader().getResource("wordcount").getPath();
//        DataStream<String> sourceStream = env.readTextFile(inPath);


        DataStream<String> sourceStream = env.socketTextStream("localhost", 8888, "\n");


        /***
         *      3. 对于流处理来说也拥有flatMap方法, 并且我们可以沿用之前批处理的类, FlatMapFunction接口是公共的
         *         但是对于聚合方法, 流处理中使用keyBy
         *
         */
        DataStream<Tuple2<String, Integer>> resultStream = sourceStream.flatMap(new WordCountBatch.WordCountFlatMapFunction())
                .keyBy(0)
                .sum(1);

        resultStream.print();
        env.execute();
    }
}
