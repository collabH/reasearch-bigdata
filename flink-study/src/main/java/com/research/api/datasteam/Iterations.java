/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.datasteam;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: Iterations.java
 * @description: 迭代器
 * @author: by echo huang
 * @date: 2020-02-16 11:18
 */
public class Iterations {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取一个迭代流
        IterativeStream<Integer> iterate = env.fromElements(1, 2, 3, 4, 5).iterate();
        //处理迭代流
        DataStream<Integer> data = iterate.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * 10;
            }
        });
        data.print();
        //关闭迭代流
        iterate.closeWith(data);


    }
}
