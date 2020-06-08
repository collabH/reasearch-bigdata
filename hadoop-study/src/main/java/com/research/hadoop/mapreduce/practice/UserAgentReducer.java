/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.hadoop.mapreduce.practice;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @fileName: UserAgentReducer.java
 * @description: UserAgentReducer.java类说明
 * @author: by echo huang
 * @date: 2020-02-12 10:57
 */
public class UserAgentReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
