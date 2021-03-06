package com.research.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @fileName: ReduceOutputCompress.java
 * @description: ReduceOutputCompress.java类说明
 * @author: by echo huang
 * @date: 2020-07-25 17:16
 */
public class ReduceOutputCompress extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        conf.setClass("mapreduce.output.fileoutputformat.compress.codec", DefaultCodec.class, CompressionCodec.class);
        Job job = Job.getInstance(conf);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        return 0;
    }
}
