package com.moyu.flink.examples.state;

import com.moyu.flink.examples.model.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

public class OperatorStateFunction {

    // flink run -s hdfs://localhost:9000/checkpoint/17a49e53ba97c72c99eea5db061b4024/chk-7/_metadata -p 4 -c com.moyu.flink.examples.state.OperatorStateFunction /Users/bairong/Documents/code/java/flink-examples/target/flink-examples-1.0-SNAPSHOT.jar

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setStateBackend(new FsStateBackend("hdfs://localhost:50070/checkpoint"));

        // 每秒执行一次checkpoint
        env.enableCheckpointing(10000);

        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 如果我们使用canal取消执行job那么checkpoint会被清除掉, 需要使用RETAIN_ON_CANCELLATION策略, 如果是任务运行失败则无需配置任何策略都可以
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);




        DataStreamSource<String> socketTextStream =
                env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> result = socketTextStream.filter(str -> !str.equals("") && str.split(",").length == 3)
                .map(str -> {
                    String[] fields = str.split(",");
                    return new Student(Integer.parseInt(fields[0]), fields[1], Double.parseDouble(fields[2]));
                }).returns(Student.class).map(new MyCheckpointedFunctionOperatorStateMap());

        result.print("result");

        env.execute("OperatorStateFunction Test Job");
    }

    private static class MyCheckpointedFunctionOperatorStateMap implements CheckpointedFunction,
            MapFunction<Student, Integer> {

        private Integer count = 0;
        private ListState<Integer> opCntState;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 本地变量更新算子状态
            opCntState.clear();
            opCntState.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 初始化算子状态
            ListStateDescriptor<Integer> listStateDescriptor =
                    new ListStateDescriptor<Integer>("opCnt", Integer.class);
            opCntState = context.getOperatorStateStore().getListState(listStateDescriptor);

            // 通过算子状态初始化本地变量
            Iterator<Integer> iterator = opCntState.get().iterator();
            while (iterator.hasNext()) {
                count += iterator.next();
            }

            System.out.println("init count = " + count);
        }

        @Override
        public Integer map(Student value) throws Exception {
            ++ count;
            return count;
        }
    }
}
