package com.moyu.flink.examples.state;

import com.moyu.flink.examples.model.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class OperatorState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<String> socketTextStream =
                env.socketTextStream("localhost", 8888);


        SingleOutputStreamOperator<Integer> result = socketTextStream.filter(str -> !str.equals("") && str.split(",").length == 3)
                .map(str -> {
                    String[] fields = str.split(",");
                    return new Student(Integer.parseInt(fields[0]), fields[1], Double.parseDouble(fields[2]));
                }).returns(Student.class).map(new MyOperatorStateMap());

        result.print("");


        env.execute("OperatorState Test Job");
    }

    private static class MyOperatorStateMap implements MapFunction<Student, Integer>, ListCheckpointed<Integer> {

        //

        private Integer count = 0;

        @Override
        public Integer map(Student value) throws Exception {
            ++ count ;
            return count;
        }


        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            System.out.println(Thread.currentThread().getName() + " checkpointId: " + checkpointId + " timestamp: " + timestamp);
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            System.out.println(Thread.currentThread().getName() + " state: " + state);
            for (Integer num : state) {
                count += num;
            }
        }
    }
}
