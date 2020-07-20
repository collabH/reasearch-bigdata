package com.research.hadoop.inputformat.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @fileName: KVReducer.java
 * @description: KVReducer.java类说明
 * @author: by echo huang
 * @date: 2020-07-20 23:57
 */
public class KVReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        AtomicInteger sum = new AtomicInteger();
        values.forEach(v -> sum.addAndGet(v.get()));
        context.write(key, new IntWritable(sum.intValue()));
    }
}
