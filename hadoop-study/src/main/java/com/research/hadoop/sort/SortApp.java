package com.research.hadoop.sort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @fileName: SortApp.java
 * @description: SortApp.java类说明
 * @author: by echo huang
 * @date: 2020-03-27 09:35
 */
public class SortApp extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "sort-job");
        FileSystem fs = FileSystem.get(URI.create("/user/sort"), getConf());
        if (fs.exists(new Path("/user/sort"))) {
            fs.delete(new Path("/user/sort"), true);
        }

        job.setJarByClass(SortApp.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(SortMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);

        FileInputFormat.addInputPath(job, new Path("/user/air.txt"));
        SequenceFileOutputFormat.setOutputPath(job, new Path("/user/sort"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new SortApp(), args);
        System.exit(exit);
    }
}
