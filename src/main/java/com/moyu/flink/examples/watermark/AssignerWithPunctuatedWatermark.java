package com.moyu.flink.examples.watermark;


import com.moyu.flink.examples.model.Order;
import com.moyu.flink.examples.utils.DateUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.ParseException;

/***
 *      AssignerWithPunctuatedWatermarks程序测试
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
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Order lastElement, long extractedTimestamp) {

                // 只有id为1的数据才生成WaterMark
                if (lastElement.getId() == 1) {
                    Watermark watermark = new Watermark(extractedTimestamp - 3000);
                    return watermark;
                }
                return null;
            }

            @Override
            public long extractTimestamp(Order element, long recordTimestamp) {
                try {
                    long createtime = DateUtils.strToTimestamp(element.getCreatetime());
                    return createtime;
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });

        env.execute("AssignerWithPunctuatedWatermark Test Job");
    }
}
