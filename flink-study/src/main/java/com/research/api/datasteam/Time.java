/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.datasteam;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: Time.java
 * @description: Data Streaming time 使用
 * @author: by echo huang
 * @date: 2020-02-16 17:00
 */
public class Time {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定流式时间特性:event time\processing time\Ingestion time,默认是processing time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    }
}
