package com.moyu.flink.examples.watermark;


import com.moyu.flink.examples.model.Order;
import com.moyu.flink.examples.utils.DateUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.util.Iterator;

/***
 *      AssignerWithPeriodicWatermarks程序测试
 */

public class AssignerWithPunctuatedWatermark {
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



        orderOperator.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Order>() {

            long maxTs = Long.MIN_VALUE;

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Order lastElement, long extractedTimestamp) {
                Watermark watermark = new Watermark(maxTs - 3000);
                System.out.println("maxTs: " + extractedTimestamp);
                System.out.println("extractedTimestamp: " + extractedTimestamp);
                System.out.println("watermark: " + watermark.getTimestamp());
                return watermark;
            }

            @Override
            public long extractTimestamp(Order element, long recordTimestamp) {
                try {
                    System.out.println("recordTimestamp: " + recordTimestamp);
                    long currentTimeStamp = DateUtils.strToTimestamp(element.getCreatetime());
                    maxTs = Math.max(maxTs, currentTimeStamp);
                    return maxTs;
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                return 0;
            }
        }).keyBy(order -> order.getId()).timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<Order, String, Integer, TimeWindow>() {
            @Override
            public void process(Integer integer, Context context, Iterable<Order> elements, Collector<String> out) throws Exception {
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


        env.execute("AssignerWithPunctuatedWatermark Test Job");
    }
}
