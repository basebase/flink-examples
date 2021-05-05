package com.moyu.flink.examples.source;

import com.moyu.flink.examples.model.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义数据源
 */
public class UserDefinedSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> studentSource = env.addSource(new StudentSource());
        studentSource.print();
        env.execute("User Defined Source Test Job");

    }

    private static class StudentSource implements SourceFunction<Student> {

        // 使用一个标志位, run方法判断当前标志位进行终止
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            Random r = new Random();
            // 当调用canal方法后退出循环
            while (isRunning) {
                int id = r.nextInt();
                String name = "student_" + id;
                double score = r.nextDouble();
                // 通过SourceContext进行采集数据
                ctx.collect(new Student(id, name, score));
                // 避免发送过快, 暂停3s
                Thread.sleep(3000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            System.out.println("终止发送");
        }
    }
}
