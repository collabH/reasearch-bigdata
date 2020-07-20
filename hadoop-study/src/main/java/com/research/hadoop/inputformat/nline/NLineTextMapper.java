package com.research.hadoop.inputformat.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: KVTextMapper.java
 * @description: KVTextMapper.java类说明
 * @author: by echo huang
 * @date: 2020-07-20 23:48
 */
public class NLineTextMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable count = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values = value.toString();
        String[] valueArr = values.split(",");
        for (String word : valueArr) {
            context.write(new Text(word), count);
        }
    }
}
