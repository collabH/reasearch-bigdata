package com.research.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * @fileName: MapOutputCompress.java
 * @description: map端输出开启压缩
 * @author: by echo huang
 * @date: 2020-07-25 17:13
 */
public class MapOutputCompress extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.setClass("mapreduce.map.output.compress.codec", DefaultCodec.class, CompressionCodec.class);
        Job job = Job.getInstance(conf);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
