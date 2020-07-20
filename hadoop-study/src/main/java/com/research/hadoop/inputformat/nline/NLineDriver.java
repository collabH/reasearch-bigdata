package com.research.hadoop.inputformat.nline;

import com.research.hadoop.inputformat.kv.KVReducer;
import com.research.hadoop.inputformat.kv.KVTextMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @fileName: KVDriver.java
 * @description: KVDriver.java类说明
 * @author: by echo huang
 * @date: 2020-07-20 23:59
 */
public class NLineDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new NLineDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        NLineInputFormat.setNumLinesPerSplit(job, 2);
        job.setInputFormatClass(NLineInputFormat.class);

        //设置驱动类
        job.setJarByClass(NLineDriver.class);
        //设置Mapper
        job.setMapperClass(NLineTextMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Reduce
        job.setReducerClass(NLineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setJobName("kvtext");
        //设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
