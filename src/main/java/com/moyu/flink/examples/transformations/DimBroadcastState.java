package com.moyu.flink.examples.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

/***
 *     通过broadcast state实现维度表
 */
public class DimBroadcastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();


        // 1. 创建两个数据源
        // 1.1 订单数据源, 主流数据
        /**
         *
                 1,1001,袜子,2021-05-11 11:11:11,北京,1
                 2,1002,鞋子,2021-05-11 11:11:11,上海,1
                 3,1001,袜子,2021-05-11 15:11:11,南京,1
                 4,1001,袜子,2021-05-11 12:11:11,苏州,1
                 5,1003,裤子,2021-05-12 17:11:11,杭州,1
                 6,1004,连衣裙,2021-05-21 11:11:11,山东,1
                 7,1005,手套,2021-05-19 18:21:21,黑龙江,1
                 8,1006,茅台,2021-05-11 11:11:11,俄罗斯,1
                 9,1007,虫草,2021-05-11 11:11:11,日本,1
                 10,1001,袜子,2021-09-11 11:11:11,四川,1

         */
        DataStream<Order> orderStream = env.socketTextStream("localhost", 8888).filter(Objects::nonNull)
                // 对传输数据进行处理包装成订单对象
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        String[] orderInfos = value.split(",");
                        return new Order(
                                Integer.parseInt(orderInfos[0]),
                                Integer.parseInt(orderInfos[1]),
                                orderInfos[2],
                                orderInfos[3],
                                orderInfos[4],
                                Boolean.parseBoolean(orderInfos[5]));
                    }
                });

        // 1.2 用户数据源, 广播数据
        /**
         *
                 1,1,张三,1
                 2,2,李四,1
                 3,3,王五,1
                 4,4,赵六,1
                 5,5,厉飞雨,1
                 6,6,韩天尊,1
                 7,7,氢原子,1
                 8,8,大乘期,1
         */
        SingleOutputStreamOperator<User> userStream = env.socketTextStream("localhost", 9999).filter(Objects::nonNull)
                // 对传输数据进行处理, 包装成用户对象
                .map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                String[] users = value.split(",");
                return new User(
                        Integer.parseInt(users[0]),
                        Integer.parseInt(users[1]),
                        users[2],
                        Integer.parseInt(users[3]));
            }
        });


        // 使用 MapStateDescriptor 来描述并创建 broadcast state 在下游的存储结, 仅支持MapState
        MapStateDescriptor<Integer, User> userMapStateDescriptor =
                new MapStateDescriptor<Integer, User>("userState", Integer.class, User.class);


        // Non-Keyed Stream实现
//        orderStream.connect(userStream.broadcast(userMapStateDescriptor))
//                .process(new BroadcastProcessFunction<Order, User, Tuple2<Order, User>>() {
//                    @Override
//                    public void processElement(Order value, ReadOnlyContext ctx, Collector<Tuple2<Order, User>> out) throws Exception {
//                        ReadOnlyBroadcastState<Integer, User> broadcastState =
//                                ctx.getBroadcastState(userMapStateDescriptor);
//
//                        // 从状态中获取订单id的用户信息
//                        User user = broadcastState.get(value.getId());
//                        out.collect(new Tuple2<>(value, user));
//                    }
//
//                    @Override
//                    public void processBroadcastElement(User value, Context ctx, Collector<Tuple2<Order, User>> out) throws Exception {
//                        BroadcastState<Integer, User> broadcastState = ctx.getBroadcastState(userMapStateDescriptor);
//                        // 新注册用户, 添加到状态中, 用于后续关联
//                        broadcastState.put(value.getOrderId(), value);
//                    }
//                }).print();


        // Keyed Stream实现
        orderStream.keyBy(new KeySelector<Order, Integer>() {
            @Override
            public Integer getKey(Order value) throws Exception {
                // 对数据流按照订单id进行分组
                return value.getId();
            }
        }).connect(userStream.broadcast(userMapStateDescriptor)).process(new KeyedBroadcastProcessFunction<Integer, Order, User, Tuple2<Order, User>>() {

            /***
             * 数据流的处理, 注意该处理方法只能读取broadcast state数据没有权限进行修改
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(Order value, ReadOnlyContext ctx, Collector<Tuple2<Order, User>> out) throws Exception {
                // 这里的getBroadcastState方法传入的MapStateDescriptor必须和userStream.broadcast()方法传入参数相同userMapStateDescriptor
                ReadOnlyBroadcastState<Integer, User> broadcastState = ctx.getBroadcastState(userMapStateDescriptor);
                User user = broadcastState.get(value.getId());
                out.collect(new Tuple2<>(value, user));
            }

            /**
             * 广播流数据, 这里可以对broadcast state数据进行读取和修改权限, 也只能通过这进行修改, 如果任意地方可以进行修改就无法保证数据一致性
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(User value, Context ctx, Collector<Tuple2<Order, User>> out) throws Exception {
                // 这里的getBroadcastState方法传入的MapStateDescriptor必须和userStream.broadcast()方法传入参数相同userMapStateDescriptor
                BroadcastState<Integer, User> broadcastState = ctx.getBroadcastState(userMapStateDescriptor);
                broadcastState.put(value.getOrderId(), value);
            }
        }).print();


        env.execute("DimBroadcast Operator Test Job");
    }

    /***
     *      订单信息
     */
    private static class Order {
        private Integer id;         // 订单id
        private Integer goodsId;    // 商品id
        private String goodsName;   // 商品名称
        private String createtime;  // 订单时间
        private String city;        // 订单地址
        private boolean status;     // 订单状态

        public Order() { }

        public Order(Integer id, Integer goodsId, String goodsName, String createtime, String city, boolean status) {
            this.id = id;
            this.goodsId = goodsId;
            this.goodsName = goodsName;
            this.createtime = createtime;
            this.city = city;
            this.status = status;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "id=" + id +
                    ", goodsId=" + goodsId +
                    ", goodsName='" + goodsName + '\'' +
                    ", createtime='" + createtime + '\'' +
                    ", city='" + city + '\'' +
                    ", status=" + status +
                    '}';
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public Integer getGoodsId() {
            return goodsId;
        }

        public void setGoodsId(Integer goodsId) {
            this.goodsId = goodsId;
        }

        public String getGoodsName() {
            return goodsName;
        }

        public void setGoodsName(String goodsName) {
            this.goodsName = goodsName;
        }

        public String getCreatetime() {
            return createtime;
        }

        public void setCreatetime(String createtime) {
            this.createtime = createtime;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public boolean isStatus() {
            return status;
        }

        public void setStatus(boolean status) {
            this.status = status;
        }
    }

    private static class User {
        private Integer id;         // 用户id
        private Integer orderId;    // 订单id
        private String name;        // 用户名称
        private Integer sex;         // 用户性别

        public User() { }

        public User(Integer id, Integer orderId, String name, Integer sex) {
            this.id = id;
            this.orderId = orderId;
            this.name = name;
            this.sex = sex;
        }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", orderId=" + orderId +
                    ", name='" + name + '\'' +
                    ", sex=" + sex +
                    '}';
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public Integer getOrderId() {
            return orderId;
        }

        public void setOrderId(Integer orderId) {
            this.orderId = orderId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getSex() {
            return sex;
        }

        public void setSex(Integer sex) {
            this.sex = sex;
        }
    }
}
