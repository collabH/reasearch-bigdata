package com.research.hadoop.mapreduce.air;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.BitSet;


/**
 * @fileName: AirMapReduceDriver.java
 * @description: 气温检测系统
 * @author: by echo huang
 * @date: 2020-03-16 18:24
 */
public class AirMapReduceDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "air-job");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/user/max.txt"))) {
            fileSystem.delete(new Path("/user/max.txt"), true);
            System.out.println("output file exists");
        }
        job.setJarByClass(AirMapReduceDriver.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(AirReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/user/air.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/user/max.txt"));
        //设置为true数据可以输出到控制台
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        BitSet bitSet = new BitSet();
    }
}
