package com.research.hadoop.sizedata.cache;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

/**
 * @fileName: DistributedCacheDriver.java
 * @description: Hadoop分布式缓存使用
 * @author: by echo huang
 * @date: 2020-03-27 16:38
 */
public class DistributedCacheDriver extends Configured implements Tool {

    static class StationTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = new String(value.getBytes(), Charsets.UTF_8).split(",");
            context.write(new Text(tokens[0]), new IntWritable(Integer.valueOf(tokens[1])));
        }
    }

    static class CacheReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Integer a;

        /**
         * 该方式缓存side data
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            a = 10;
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int minTemp = a;
            for (IntWritable value : values) {
                minTemp = Math.max(value.get(), minTemp);
            }
            context.write(key, new IntWritable(minTemp));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        String outpath = "/user/cache";
        FileSystem fs = FileSystem.get(URI.create(outpath), getConf());
        if (fs.exists(new Path(outpath))) {
            fs.delete(new Path(outpath), true);
        }
        job.setJarByClass(getClass());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(StationTemperatureMapper.class);
        job.setReducerClass(CacheReducer.class);
        job.addCacheFile(URI.create("/user/air.txt"));
        FileInputFormat.addInputPath(job, new Path("/cache/air.txt"));
        FileOutputFormat.setOutputPath(job, new Path(outpath));

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new DistributedCacheDriver(), args);
        System.exit(exit);
    }
}
