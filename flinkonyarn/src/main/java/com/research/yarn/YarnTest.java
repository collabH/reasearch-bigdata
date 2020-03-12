/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.yarn;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @fileName: YarnTest.java
 * @description: YarnTest.java类说明
 * @author: by echo huang
 * @date: 2020-02-23 20:16
 */
public class YarnTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("hello", "world", "zhangsan");
        data.map(new MyMapper())
                .writeAsText("/Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/text.txt");
        env.execute();
    }
}
