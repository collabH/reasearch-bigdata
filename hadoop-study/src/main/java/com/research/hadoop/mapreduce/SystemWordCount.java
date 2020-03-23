package com.research.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;

import java.io.IOException;

/**
 * @fileName: SystemWordCount.java
 * @description: SystemWordCount.java类说明
 * @author: by echo huang
 * @date: 2020-03-16 16:23
 */
public class SystemWordCount {
    public static void main(String[] args) throws IOException {
        Configuration config = new Configuration();
        FileSystem fileSystem = FileSystem.get(config);
        Job job = Job.getInstance(config, "wordCount");

        job.setMapperClass(TokenCounterMapper.class);
    }
}
