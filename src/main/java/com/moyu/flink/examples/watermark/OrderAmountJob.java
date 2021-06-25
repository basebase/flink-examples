package com.moyu.flink.examples.watermark;

import com.moyu.flink.examples.model.Order;
import com.moyu.flink.examples.utils.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.time.Duration;
import java.util.Iterator;

/***
 *      统计每个窗口下订单的金额信息
 */

public class OrderAmountJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Order> orderOperator = socketSource.filter(str -> !str.equals("") && str.split(",").length == 4)
                .map(str -> {
                    String[] fields = str.split(",");
                    return new Order(Integer.parseInt(fields[0]), fields[1], Integer.parseInt(fields[2]), fields[3]);
                });

        WatermarkStrategy<Order> orderWatermarkStrategy = WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withIdleness(Duration.ofMillis(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
            @Override
            public long extractTimestamp(Order element, long recordTimestamp) {
                try {
                    return DateUtils.strToTimestamp(element.getCreatetime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                return 0L;
            }
        });

        OutputTag<Order> lateOrder = new OutputTag<Order>("order-late"){};


        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result = orderOperator.assignTimestampsAndWatermarks(orderWatermarkStrategy)
                .keyBy(order -> order.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))   // 5秒窗口
                .allowedLateness(Time.seconds(10))  // 等待10秒
                .sideOutputLateData(lateOrder)      // 超过watermark和allowedLateness的延迟数据
                .process(new ProcessWindowFunction<Order, Tuple2<Integer, Integer>, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer key, Context context, Iterable<Order> elements, Collector<Tuple2<Integer, Integer>> out) throws Exception {

                        TimeWindow window = context.window();

                        System.out.println("=========================== window start (key: " + (key) + ")" + window.getStart() + " ===========================");

                        int amount = 0;

                        // 窗口内的数据输出
                        Iterator<Order> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            Order order = iterator.next();
                            System.out.println(order);
                            amount += order.getAmount();
                        }

                        System.out.println("=========================== window end (key: " + (key) + ")" + window.getEnd() + " ===========================");

                        out.collect(new Tuple2<>(key, amount));
                    }
                });

        result.print("OrderAmount");

        // 获取侧输出流数据
        result.getSideOutput(lateOrder).print("late-order");

        env.execute("OrderAmount Test Job");
    }
}
