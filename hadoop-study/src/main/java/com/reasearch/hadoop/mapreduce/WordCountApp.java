/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.reasearch.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @fileName: WordCountApp.java
 * @description: 使用MapReduce开发WordCount程序
 * @author: by echo huang
 * @date: 2020-02-11 11:53
 */
public class WordCountApp {
    /**
     * Driver:封装MapReduce作业的所有信息
     * 编译打包上传至hadoop服务器，然后用yarn进行执行
     * hadoop jar jarname 主类 2 2
     *
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String jobName = "mapreduce-wordCount";
        Configuration configuration = new Configuration();

        //创建Job
        Job job = Job.getInstance(configuration, jobName);

        //设置Job处理类
        job.setJarByClass(WordCountApp.class);
        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置自定义的Mapper处理类和Reducer处理类以及对应输出参数类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置作业处理的输出路径
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
