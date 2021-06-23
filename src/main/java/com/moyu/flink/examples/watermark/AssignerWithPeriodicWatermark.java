package com.moyu.flink.examples.watermark;


import com.moyu.flink.examples.model.Order;
import com.moyu.flink.examples.utils.DateUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.Iterator;

/***
 *      AssignerWithPeriodicWatermarks程序测试
 */

public class AssignerWithPeriodicWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Order> orderOperator = socketSource.filter(str -> !str.equals("") && str.split(",").length == 4)
                .map(str -> {
                    String[] fields = str.split(",");
                    return new Order(Integer.parseInt(fields[0]), fields[1], Integer.parseInt(fields[2]), fields[3]);
                });


        /***
         *      核心点, 不过该方法在1.11中已经被废弃了, 在使用idea运行本该运行的window没有被触发计算, 而提交到flink中会触发, 有点奇怪?
         */
        orderOperator.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(Order element) {
                try {
                    long watermark = DateUtils.strToTimestamp(element.getCreatetime());
                    return watermark;
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                return 0;
            }
        }).keyBy(order -> order.getId()).timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<Order, String, Integer, TimeWindow>() {
            @Override
            public void process(Integer key, Context context, Iterable<Order> elements, Collector<String> out) throws Exception {

                TimeWindow window = context.window();
                long windowStart = window.getStart();
                long windowEnd = window.getEnd();
                System.out.println("=======================windowStart: " + (windowStart) + "~" + " windowEnd: " + windowEnd +  "===========================");
                Iterator<Order> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    out.collect(iterator.next().toString());
                }
            }
        }).print();


        env.execute("AssignerWithPeriodicWatermark Test Job");
    }
}
