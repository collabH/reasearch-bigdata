package com.research.training;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: MySqlSourceTest.java
 * @description: MySqlSourceTest.java类说明
 * @author: by echo huang
 * @date: 2020-03-01 16:58
 */
public class MySqlSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new MySqlSource()).print();
        env.execute("test mysqlsource");
    }
}
