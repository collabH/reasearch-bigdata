package com.research.hadoop.mr.sort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @fileName: SortPartitioner.java
 * @description: SortPartitioner.java类说明
 * @author: by echo huang
 * @date: 2020-03-27 10:12
 */
public class SortPartitioner extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "sort-path");
        FileSystem fs = FileSystem.get(URI.create("/user/sort/path"), getConf());
        if (fs.exists(new Path("/user/sort/path"))) {
            fs.delete(new Path("/user/sort/path"), true);
        }

        job.setJarByClass(SortPartitioner.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        FileInputFormat.addInputPath(job, new Path("/user/air.txt"));
        SequenceFileOutputFormat.setOutputPath(job, new Path("/user/sort/path"));
        job.setNumReduceTasks(3);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SortPartitioner(), args));
    }
}
