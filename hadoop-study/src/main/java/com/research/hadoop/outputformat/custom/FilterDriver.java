package com.research.hadoop.outputformat.custom;

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
 * @fileName: FilterDriver.java
 * @description: FilterDriver.java类说明
 * @author: by echo huang
 * @date: 2020-07-25 11:25
 */
public class FilterDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());


        job.setJarByClass(getClass());


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapperClass(FilterMapper.class);


        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(CustomOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/Users/babywang/Desktop/input/test.txt"));

        // 虽然自定义了outputformat，但是outputformat继承fileoutputformat，而fileoutputformat要输出一个_SUCCESS文件，所以这里还需要指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("/Users/babywang/Desktop/output"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new FilterDriver(), args);
        System.exit(run);
    }
}
