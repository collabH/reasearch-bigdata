package com.research.hadoop.air;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @fileName: AirReduce.java
 * @description: AirReduce.java类说明
 * @author: by echo huang
 * @date: 2020-03-16 18:31
 */
public class AirReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxAir = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            maxAir = Math.max(value.get(), maxAir);
        }
        Counter counter = context.getCounter("air", "test");
        counter.increment(1);
        System.out.println("job count:" + counter.getValue());
        context.write(key, new IntWritable(maxAir));
    }
}
