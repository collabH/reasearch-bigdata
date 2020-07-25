package com.research.hadoop.mutiljob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @fileName: OneJobMapper.java
 * @description: 多job串联
 * @author: by echo huang
 * @date: 2020-07-25 23:41
 */
public class OneJobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private String fileName;

    private Text k = new Text();
    private IntWritable num = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lineArr = line.split(",");
        for (String word : lineArr) {
            k.set(word + "--" + fileName);
            context.write(k, num);
        }
    }
}
