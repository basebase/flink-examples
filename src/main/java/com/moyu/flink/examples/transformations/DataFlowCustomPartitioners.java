package com.moyu.flink.examples.transformations;

import com.moyu.flink.examples.model.Student;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 *          自定义分区
 */
public class DataFlowCustomPartitioners {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource =
                env.socketTextStream("localhost", 8888);


        socketSource.map(value -> {
            String[] fields = value.split(",");
            return new Student(Integer.parseInt(fields[0]), fields[1], Double.parseDouble(fields[2]));
        }).name("studentMap").setParallelism(2)
                .partitionCustom(new StudentPartitioner(), new StudentKeySelector())
                .map(value -> value.toString().toUpperCase()).name("upStrMap").setParallelism(4).print();


        env.execute("DataFlow Custom Partition Test Job");
    }

    private static class StudentPartitioner implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            if (key < 0) {
                return 0;
            } else if (key > 0 && key < 10) {
                return 1;
            } else if (key > 10 && key < 20) {
                return 2;
            }
            return 3;
        }
    }

    private static class StudentKeySelector implements KeySelector<Student, Integer> {
        @Override
        public Integer getKey(Student value) throws Exception {
            return value.getId();
        }
    }
}
