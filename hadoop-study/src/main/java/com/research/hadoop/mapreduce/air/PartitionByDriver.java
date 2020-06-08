package com.research.hadoop.mapreduce.air;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @fileName: PartitionByDriver.java
 * @description: PartitionByDriver.java类说明
 * @author: by echo huang
 * @date: 2020-03-26 16:34
 */
public class PartitionByDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "air-job");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path("/user/max"))) {
            fileSystem.delete(new Path("/user/max"), true);
            System.out.println("output file exists");
        }
        job.setJarByClass(AirMapReduceDriver.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(AirReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //开启延迟加载
        job.setOutputFormatClass(LazyOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/user/air.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/user/max"));
        //设置为true数据可以输出到控制台
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int count = ToolRunner.run(new PartitionByDriver(), args);
        System.exit(count);
    }
}
