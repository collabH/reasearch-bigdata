/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.yarn;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: YarnTest.java
 * @description: YarnTest.java类说明
 * @author: by echo huang
 * @date: 2020-02-23 20:16
 */
public class YarnTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        data.print();
        env.execute("flink on yarn test");
    }
}
