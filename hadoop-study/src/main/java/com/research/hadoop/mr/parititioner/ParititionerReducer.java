/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.hadoop.mr.parititioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @fileName: ParititionerReducer.java
 * @description: ParititionerReducer.java类说明
 * @author: by echo huang
 * @date: 2020-02-11 17:56
 */
public class ParititionerReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
