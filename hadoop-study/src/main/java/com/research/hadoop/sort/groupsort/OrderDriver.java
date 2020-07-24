package com.research.hadoop.sort.groupsort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @fileName: OrderDriver.java
 * @description: OrderDriver.java类说明
 * @author: by echo huang
 * @date: 2020-07-24 22:42
 */
public class OrderDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new OrderDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "groupSort");

        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderDetail.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderDetail.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/Users/babywang/Desktop/input/test.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/babywang/Desktop/output"));
        // 添加分组函数，基于某个id为key
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }


}
