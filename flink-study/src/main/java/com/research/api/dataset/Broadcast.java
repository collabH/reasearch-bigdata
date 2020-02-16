/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.dataset;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * @fileName: Broadcast.java
 * @description: 广播变量
 * @author: by echo huang
 * @date: 2020-02-15 16:19
 */
public class Broadcast {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);

        DataSet<String> data = env.fromElements("a", "b");

        data.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                Collection<Integer> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
                broadcastSet.forEach(System.out::println);
            }


            @Override
            public String map(String value) throws Exception {
                return "wc:" + value;
            }
        }).withBroadcastSet(toBroadcast, "broadcastSetName").print();
    }

}
