package com.moyu.flink.examples.wordcount.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        // 1. 获取上下文(执行环境), 没有这个是无法执行flink程序的
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文本数据
        /***
         *      2. 读取文本数据, 返回了一个DataSource类型, 一个数据源;
         *         但是, 我们深入DataSource类可以发现该类继承了Operator类, 而Operator类又继承了DataSet类
         *         所以最本质处理的数据集是一个DataSet所以flink的批处理API被称为DataSet API
         *
         *         一般DataSet处理有限的数据集
         */
        String inPath = WordCountBatch.class.getClassLoader().getResource("wordcount").getPath();

//        DataSource<String> dataSource = env.readTextFile("");
        DataSet<String> source = env.readTextFile(inPath);

        /**
         *      3. 使用flatMap方法将数据转换成<K,V>形式之后, 但是每个key的值都是1, 所以我们还需要进行分组
         *         所以我们需要使用groupBy方法, 想要使用flink的分组, 我们可以使用三种方式:
         *              1. 实现KeySelector
         *              2. 传入位置
         *              3. 传入字段名称
         *         由于我们使用的是元组类型, 我们并不清楚具体的字段名称也并不想实现KeySelector接口, 所以采用第二种方法传入具体位置
         *
         */
        DataSet<Tuple2<String, Integer>> resultSet = source.flatMap(new WordCountFlatMapFunction())
                .groupBy(0)       // 按照第一个值分组Tuple2.f0就是我们的token, 也就是按照token分组
                .sum(1);// 这里进行sum统计使用Tuple2.f1的值

        resultSet.print();
//        env.execute("WordCount Batch Job");
    }

    /***
     *     3.1: 自定义flatMap函数, 用于切分每条数据
     *      注意这里使用的Tuple2二元组使用的java的版本而不是scala的版本, 所以导入的包是org.apache.flink.api.java.tuple.Tuple2
     */
    public static class WordCountFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            if (s == null || s.length() == 0)
                return;
            String[] tokens = s.split(",");
            for (String token : tokens) {
                // 产生的多条数据都被collector进行收集
                collector.collect(new Tuple2<>(token, 1));      // 每个单词都固定是1
            }
        }
    }
}