package com.research.hadoop.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: SortMapper.java
 * @description: SortMapper.java类说明
 * @author: by echo huang
 * @date: 2020-03-26 18:54
 */
public class SortMapper extends Mapper<LongWritable, Text,LongWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
