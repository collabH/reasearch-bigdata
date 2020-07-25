package com.research.hadoop.join.map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @fileName: MapJoinDriver.java
 * @description: MapJoinDriver.java类说明
 * @author: by echo huang
 * @date: 2020-07-25 14:28
 */
public class MapJoinDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(getClass());


        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("/Users/babywang/Desktop/input/pd.txt"));

        // mapper端处理，不经过reducer阶段
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path("/Users/babywang/Desktop/input/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/babywang/Desktop/output"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new MapJoinDriver(), args);
        System.exit(run);
    }
}
