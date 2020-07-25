package com.research.hadoop.join.reduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @fileName: TableDriver.java
 * @description: TableDriver.java类说明
 * @author: by echo huang
 * @date: 2020-07-25 13:46
 */
public class TableDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());

        job.setMapperClass(TableMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);


        job.setReducerClass(TableReducer.class);
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.addInputPath(job, new Path("/Users/babywang/Desktop/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/babywang/Desktop/output"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new TableDriver(), args);
        System.exit(run);
    }
}
