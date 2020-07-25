package com.research.hadoop.mutiljob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: TwoJobMapper.java
 * @description: TwoJobMapper.java类说明
 * @author: by echo huang
 * @date: 2020-07-26 00:12
 */
public class TwoJobMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // hsm-a.txt 3
        // wy-b.txt 2

        String line = value.toString();
        String[] fields = "--".split(line);
        k.set(fields[0]);
        v.set(fields[1]);
        context.write(k, v);
    }
}
