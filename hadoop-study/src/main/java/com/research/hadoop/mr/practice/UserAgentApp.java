///*
// * Copyright: 2020 forchange Inc. All rights reserved.
// */
//
//package com.research.hadoop.mr.practice;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import java.io.IOException;
//
///**
// * @fileName: UserAgentApp.java
// * @description: 统计nginx的useragent日志，对应浏览器的数量
// * @author: by echo huang
// * @date: 2020-02-12 10:58
// */
//public class UserAgentApp {
//    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration configuration = new Configuration();
//
//        //准备清理已经存在的输出目录
//        Path path = new Path(args[1]);
//        FileSystem fileSystem = FileSystem.get(configuration);
//        if (fileSystem.exists(path)) {
//            fileSystem.delete(path, true);
//            System.out.println("output file exists");
//        }
//
//        Job job = Job.getInstance(configuration, "useragent");
//
//        job.addArchiveToClassPath(new Path("/jar/userAgentPar.jar"));
//        job.setNumReduceTasks(4);
//        job.setCombinerClass(UserAgentReducer.class);
//
//        job.setMapOutputValueClass(LongWritable.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);
//
//        job.setMapperClass(UserAgentMapper.class);
//        job.setReducerClass(UserAgentReducer.class);
//
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//
//        //设置作业处理的输出路径
//        TextOutputFormat.setOutputPath(job, path);
//
//        //提交作业
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//}
