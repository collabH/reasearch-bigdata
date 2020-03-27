package com.research.hadoop.sort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @fileName: SecondarySort.java
 * @description: SecondarySort.java类说明
 * @author: by echo huang
 * @date: 2020-03-27 10:50
 */
public class SecondarySort extends Configured implements Tool {
    class MaxTemplateMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }
}
