package com.research.hadoop.mapreduce.air;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;

/**
 * @fileName: Mapper.java
 * @description: Mapper.java类说明
 * @author: by echo huang
 * @date: 2020-03-16 18:24
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    enum Temperature {
        OVER_100
    }

    private static final Text YEAR_CONTEXT = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //模拟map任务异常
//        int i=1/0;
        //假设数据格式为年-温度
        String line = value.toString();
        String[] tokens = line.split("-");
        String year = tokens[0];
        int air = Integer.parseInt(tokens[1]);
        YEAR_CONTEXT.set(year);
        Counter counter = context.getCounter("air","test");
        counter.increment(1);
        System.out.println("job map count:" + counter.getValue());
        context.write(YEAR_CONTEXT, new IntWritable(air));
    }
}
