package com.research.hadoop.mapreduce.local;

import com.research.hadoop.air.AirReduce;
import com.research.hadoop.air.Mapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @fileName: LocalRun.java
 * @description: 本地机器运行
 * @author: by echo huang
 * @date: 2020-03-22 19:11
 */
public class LocalRun extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("参数异常");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Job job = Job.getInstance(getConf(), "air");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(getClass());

        job.setMapperClass(Mapper.class);
        job.setReducerClass(AirReduce.class);
        job.setCombinerClass(AirReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[2];
        args[0] = "/user/max.txt";
        args[1] = "/Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/logs/hello";
        int run = ToolRunner.run(new LocalRun(), args);

        System.exit(run);
    }
}
