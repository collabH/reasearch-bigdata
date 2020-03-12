/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.hadoop.mapreduce.parititioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @fileName: PartitionerApp.java
 * @description: 自定义路由
 * @author: by echo huang
 * @date: 2020-02-11 11:53
 */
public class PartitionerApp {
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
        //准备清理已经存在的输出目录
        Path path = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
            System.out.println("output file exists");
        }
        //创建Job
        Job job = Job.getInstance(configuration, jobName);
        //设置jar包依赖
        job.addArchiveToClassPath(new Path("hdfs://hadoop:8020/hello/commons-lang3-3.9.jar"));

        //设置Job处理类
        job.setJarByClass(PartitionerApp.class);
        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置自定义的Mapper处理类和Reducer处理类以及对应输出参数类型
        job.setMapperClass(ParititionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(ParititionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置job的partitioner
        job.setPartitionerClass(MyPartitioner.class);
        //设置4个reducer，每个分区一个
        job.setNumReduceTasks(4);

        //设置作业处理的输出路径
        TextOutputFormat.setOutputPath(job, path);

        //提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
