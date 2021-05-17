package com.moyu.flink.examples.transformations;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/***
 *  connect算子使用
 *        模拟实现join操作, 不过还不如使用join
 */
public class ConnectOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. 从socket中读取数据
        /***
         * AAA,beijing,1
         * VVV,shanghai,2
         * DDD,anhui,3
         * EEE,hunan,4
         * GGG,fujian,5
         */
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);

        // 2. 规则数据
        DataStreamSource<Tuple3<Integer, String, String>> ruleStream = env.fromCollection(Arrays.asList(
                new Tuple3<>(1, "下单成功", "2021-05-11 11:11:11"),
                new Tuple3<>(2, "下单成功", "2021-05-11 11:11:12"),
                new Tuple3<>(3, "订单未完成", "2021-05-11 11:11:13"),
                new Tuple3<>(4, "订单失败", "2021-05-11 11:11:14"),
                new Tuple3<>(5, "下单成功", "2021-05-11 11:11:15"),
                new Tuple3<>(6, "下单成功", "2021-05-11 11:11:16"),
                new Tuple3<>(4, "下单成功", "2021-05-11 11:11:17"),
                new Tuple3<>(1, "订单失败", "2021-05-11 11:11:18"),
                new Tuple3<>(7, "下单成功", "2021-05-11 11:11:19"),
                new Tuple3<>(3, "下单成功", "2021-05-11 11:11:20")
        ));

        // 3. 两个流先关联, 在分组
        socketStream.connect(ruleStream)

                /***
                 *     在map中, 我们将两条流数据都封装成java bean对象, 拥有两条流数据所有字段
                 */

                .map(new CoMapFunction<String, Tuple3<Integer,String,String>, OrderInfo>() {
                    @Override
                    public OrderInfo map1(String value) throws Exception {      // map1用来处理第一个流数据
                        String[] fields = value.split(",");
                        OrderInfo orderInfo = new OrderInfo();
                        orderInfo.setName(fields[0]);
                        orderInfo.setCity(fields[1]);
                        orderInfo.setOrderInfoId(Integer.parseInt(fields[2]));
                        return orderInfo;
                    }

                    @Override
                    public OrderInfo map2(Tuple3<Integer, String, String> value) throws Exception {     // map2用来处理第二个流数据
                        OrderInfo orderInfo = new OrderInfo();
                        orderInfo.setOrderId(value.f0);
                        orderInfo.setStatus(value.f1);
                        orderInfo.setCreatetime(value.f2);
                        return orderInfo;
                    }
                }).keyBy(new KeySelector<OrderInfo, Integer>() {
            /**
             * 按照订单id进行分组
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public Integer getKey(OrderInfo value) throws Exception {
                return value.getOrderId() == null ? value.getOrderInfoId() : value.getOrderId();
            }
        }).process(new KeyedProcessFunction<Integer, OrderInfo, OrderInfo>() {

            /**
             *     对于这里的state不用在纠结, 简单理解在内存中的数据
             */
            private ValueState<List<OrderInfo>> userValueState = null;
            private ValueState<List<OrderInfo>> orderValueState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor userValueStateDesc = new ValueStateDescriptor("userValueState", TypeInformation.of(new TypeHint<List<OrderInfo>>() {}));
                ValueStateDescriptor orderValueStateDesc = new ValueStateDescriptor("orderValueState", TypeInformation.of(new TypeHint<List<OrderInfo>>() {}));

                userValueState = getRuntimeContext().getState(userValueStateDesc);
                orderValueState = getRuntimeContext().getState(orderValueStateDesc);
            }

            @Override
            public void processElement(OrderInfo value, Context ctx, Collector<OrderInfo> out) throws Exception {

                if (value.getOrderInfoId() != null) {       // 如果当前数据是第一条流数据
                    List<OrderInfo> userList = userValueState.value();
                    List<OrderInfo> orderList = orderValueState.value();

                    if (userList == null)
                        userList = new ArrayList<>();

                    userList.add(value);

                    /***
                     *     将所有第一条用户数据都取出和订单数据进行关联
                     */

                    if (orderList != null) {
                        for (OrderInfo userInfo : userList) {
                            for (OrderInfo orderInfo : orderList) {
                                userInfo.setOrderId(orderInfo.getOrderId());
                                userInfo.setStatus(orderInfo.getStatus());
                                userInfo.setCreatetime(orderInfo.getCreatetime());
                                out.collect(userInfo);
                            }
                        }

                        // 关联后清除数据

                        userValueState.clear();
                        orderValueState.clear();
                    } else {
                        userList.add(value);
                        userValueState.update(userList);
                    }
                } else {
                    List<OrderInfo> orderList = orderValueState.value();
                    if (orderList == null)
                        orderList = new ArrayList<>();

                    orderList.add(value);
                    orderValueState.update(orderList);
                }

            }
        }).print();


        env.execute("Connect Operator Test Job");
    }


    private static class OrderInfo {
        private Integer orderInfoId;
        private Integer orderId;
        private String status;
        private String createtime;
        private String name;
        private String city;

        public OrderInfo() { }

        @Override
        public String toString() {
            return "OrderInfo{" +
                    "orderInfoId=" + orderInfoId +
                    ", orderId=" + orderId +
                    ", status='" + status + '\'' +
                    ", createtime='" + createtime + '\'' +
                    ", name='" + name + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }

        public Integer getOrderId() {
            return orderId;
        }

        public void setOrderId(Integer orderId) {
            this.orderId = orderId;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getCreatetime() {
            return createtime;
        }

        public void setCreatetime(String createtime) {
            this.createtime = createtime;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public Integer getOrderInfoId() {
            return orderInfoId;
        }

        public void setOrderInfoId(Integer orderInfoId) {
            this.orderInfoId = orderInfoId;
        }
    }
}
