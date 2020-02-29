/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.yarn;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

/**
 * @fileName: MyMapper.java
 * @description: MyMapper.java类说明
 * @author: by echo huang
 * @date: 2020-02-27 21:38
 */
public class MyMapper extends RichMapFunction<String, String> {
    private transient Counter counter;

    @Override
    public String map(String s) throws Exception {
        this.counter.inc();
        return "wx:" + s;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.counter = getRuntimeContext().getMetricGroup()
                .counter("counter1");
    }
}
