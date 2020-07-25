package com.research.hadoop.mr.counter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @fileName: App.java
 * @description: App.java类说明
 * @author: by echo huang
 * @date: 2020-03-26 17:31
 */
public class App extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "air");

        FileSystem fs = FileSystem.get(URI.create("/user/counter"), getConf());
        if (fs.exists(new Path("/user/counter"))) {
            fs.delete(new Path("/user/counter"), true);
        }
        job.setJarByClass(App.class);
        job.setMapperClass(CustomCounterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/user/air.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/user/counter"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new App(), args);
        System.exit(run);
    }
}
