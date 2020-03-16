package com.research.hadoop.air;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @fileName: Mapper.java
 * @description: Mapper.java类说明
 * @author: by echo huang
 * @date: 2020-03-16 18:24
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {


    private static final Text YEAR_CONTEXT = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //假设数据格式为年-温度
        String line = value.toString();
        String[] tokens = line.split("-");
        String year = tokens[0];
        int air = Integer.parseInt(tokens[1]);
        YEAR_CONTEXT.set(year);
        context.write(YEAR_CONTEXT, new IntWritable(air));
    }
}
