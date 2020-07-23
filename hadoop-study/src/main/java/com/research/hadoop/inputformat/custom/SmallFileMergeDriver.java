package com.research.hadoop.inputformat.custom;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @fileName: SmallFileMergeDriver.java
 * @description: SmallFileMergeDriver.java类说明
 * @author: by echo huang
 * @date: 2020-07-22 22:35
 */
public class SmallFileMergeDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "smallFileMerge");

        //设置Driver Class
        job.setJarByClass(getClass());

        //设置Mapper
        job.setMapperClass(SmallFileMergeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //设置Reduce
        job.setReducerClass(SmallFileMergeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //输出类型
        job.setInputFormatClass(CustomInputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new SmallFileMergeDriver(), args);
        System.exit(run);
    }
}
