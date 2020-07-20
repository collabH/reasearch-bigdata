package com.research.hadoop.inputformat.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: KVTextMapper.java
 * @description: KVTextMapper.java类说明
 * @author: by echo huang
 * @date: 2020-07-20 23:48
 */
public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {
    private IntWritable count = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //写出
        context.write(key, count);
    }
}
