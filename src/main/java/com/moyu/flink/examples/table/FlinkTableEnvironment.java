package com.moyu.flink.examples.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 *     Flink TableEnvironment实例创建
 *          1. old Planner
 *          2. blink Planner
 *
 *    两种Planner如何创建与使用
 *
 *
 *         1. 创建EnvironmentSettings对象
 *              EnvironmentSettings.newInstance
 *                          .build();
 *         中间使用上面planner和什么模式自行选定, 但是没有这两个则无法创建
 *
 *
 *         2. old的批处理模式和blink模式创建方式不一样
 *
 *
 */

public class FlinkTableEnvironment {
    public static void main(String[] args) {

        /***
         *      flink1.11默认使用的是blink的版本, 但是如果想使用old planner怎么做呢
         */

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 0. 这一步操作, 默认使用blink, 如果想使用old planner如何处理呢
        StreamTableEnvironment.create(env);

        // 1. 在创建TableEnvironment实例, 我们可以添加相关参数用于配置, 创建EnvironmentSettings实例
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()        // 使用 old planner
                .inStreamingMode()      // stream模式
                .build();

        // 1.1 创建一个基于old planner 的流Table环境
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);


        // 2. 创建一个基于old planner的批处理环境
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);


        /***
         *
         *      基于blink planner创建流&批的环境
         */


        // 3. 创建基于blink的流环境
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()      // 使用blink planner
                .inStreamingMode()      // stream模式
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);


        // 4. 创建基于blink的批环境
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);
    }
}
