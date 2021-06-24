package com.moyu.flink.examples.watermark;


import com.moyu.flink.examples.model.Order;
import com.moyu.flink.examples.utils.DateUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.text.ParseException;

/***
 *      AssignerWithPeriodicWatermarks程序测试
 */

public class AssignerWithPeriodicWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(3000);

        // 下面的WaterMark如果并行度为3则会创建3个对象
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Order> orderOperator = socketSource.filter(str -> !str.equals("") && str.split(",").length == 4)
                .map(str -> {
                    String[] fields = str.split(",");
                    return new Order(Integer.parseInt(fields[0]), fields[1], Integer.parseInt(fields[2]), fields[3]);
                });


        /***
         *
         *      下面两个设置watermark是1.11之前的设置方式, 新版有其它API实现
         */

        // 在无序的情况下使用, 数据可以等待延迟3s
        orderOperator.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(Order element) {
                try {
                    long createtime = DateUtils.strToTimestamp(element.getCreatetime());
                    return createtime;
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                return 0;
            }
        });



        // 在数据有序的情况下使用, 即所有数据都是顺序的, 无需等待
        orderOperator.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
            @Override
            public long extractAscendingTimestamp(Order element) {
                try {
                    long createtime = DateUtils.strToTimestamp(element.getCreatetime());
                    return createtime;
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                return 0;
            }
        });


        // 自定义生成方式
        orderOperator.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Order>() {

            private long bound = 3000;      // 3s
            private long maxTs = Long.MIN_VALUE;    // 最大时间戳

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                // 生成一个具有3s延迟的水位线
                return new Watermark(maxTs - bound);
            }

            @Override
            public long extractTimestamp(Order element, long recordTimestamp) {
                try {
                    long currentTime = DateUtils.strToTimestamp(element.getCreatetime());
                    // 更新最大时间戳
                    maxTs = Math.max(currentTime, maxTs);
                    return currentTime;
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });

        env.execute("AssignerWithPeriodicWatermark Test Job");
    }
}
