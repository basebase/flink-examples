package com.moyu.flink.examples.watermark;

import com.moyu.flink.examples.model.Order;
import com.moyu.flink.examples.utils.DateUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Iterator;

/***
 *
 *      flink1.11新版WaterMark测试
 */

public class AssignTimestampsAndWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Order> orderOperator = socketSource.filter(str -> !str.equals("") && str.split(",").length == 4)
                .map(str -> {
                    String[] fields = str.split(",");
                    return new Order(Integer.parseInt(fields[0]), fields[1], Integer.parseInt(fields[2]), fields[3]);
                });



        /***
         *     创建一个WaterMark对象, 数据延迟等待3s, 使用createtime作为传递
         *          注意: withIdleness方法, 我们之前说过, 多个分区下如果有某个分区在一段时间内未发送事件数据, 多个分区取最小值发送则会
         *               出现WaterMark不前进问题, withIdleness方法方法就是解决这个问题的, 用来检查空闲输入并将其标记为空闲状态, 不需要等待该分区WaterMark
         *               当下次有数据进入并发送到下游, 该分区又会成为活跃状态
         */
        WatermarkStrategy<Order> orderWatermarkStrategy = WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withIdleness(Duration.ofMillis(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                    @Override
                    public long extractTimestamp(Order element, long recordTimestamp) {
                        try {
                            long currentTimeStamp = DateUtils.strToTimestamp(element.getCreatetime());
                            return currentTimeStamp;
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        return 0;
                    }
                });

        KeyedStream<Order, Integer> orderIntegerKeyedStream = orderOperator.assignTimestampsAndWatermarks(orderWatermarkStrategy)
                .keyBy(order -> order.getId());


        orderIntegerKeyedStream.timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<Order, String, Integer, TimeWindow>() {
            @Override
            public void process(Integer key, Context context, Iterable<Order> elements, Collector<String> out) throws Exception {
                TimeWindow window = context.window();
                System.out.println("=============================(" + window.getStart() + "~" + window.getEnd() + ")=============================");
                Iterator<Order> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    out.collect(iterator.next().toString());
                }
            }
        }).setParallelism(1);


        env.execute("AssignTimestampsAndWatermark Test Job");
    }
}
