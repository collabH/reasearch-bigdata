package com.research.hadoop.counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: CustomCounterMapper.java
 * @description: CustomCounterMapper.java类说明
 * @author: by echo huang
 * @date: 2020-03-26 17:29
 */
public class CustomCounterMapper extends Mapper<LongWritable, Text, Text, Text> {
    enum Counter {
        HELLO,
        WORLD;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(Enum.valueOf(Counter.class, Counter.HELLO.name())).increment(1);
        context.write(new Text(key.toString()), value);
    }
}
