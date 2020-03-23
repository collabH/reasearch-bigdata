/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.hadoop.practice;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: UserAgentMapper.java
 * @description: UserAgentMapper.java类说明
 * @author: by echo huang
 * @date: 2020-02-12 10:55
 */
public class UserAgentMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private UserAgentParser parser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        parser = new UserAgentParser();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        UserAgent agent = parser.parse(line);
        context.write(new Text(agent.getBrowser()), new LongWritable(1));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        parser = null;
    }
}
