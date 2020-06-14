package dev.hive.test.etl;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @fileName: ETLDriver.java
 * @description: ETLDriver.java类说明
 * @author: by echo huang
 * @date: 2020-06-14 13:56
 */
public class ETLDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        //设置jar包路径
        job.setJarByClass(getClass());
        //设置输入类型
        job.setInputFormatClass(TextInputFormat.class);
        //设置mapper类&kv类型
        job.setMapperClass(ETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设置最终key类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //输出最终输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交任务
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ETLDriver etlDriver = new ETLDriver();
        int result = ToolRunner.run(etlDriver, args);

        System.exit(result);
    }
}
