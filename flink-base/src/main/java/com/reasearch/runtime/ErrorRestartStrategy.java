package com.reasearch.runtime;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: ErrorRestartStrategy.java
 * @description: 错误重启设置
 * @author: by echo huang
 * @date: 2020-03-10 14:31
 */
public class ErrorRestartStrategy {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //task重启策略配置
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
    }
}
