/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.hadoop.mapreduce.parititioner;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: ParititionerMapper.java
 * @description: ParititionerMapper.java类说明
 * @author: by echo huang
 * @date: 2020-02-11 17:54
 */
public class ParititionerMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = StringUtils.split(line, " ");
        context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));
    }
}
