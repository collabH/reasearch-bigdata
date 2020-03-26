package com.research.hadoop.air;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @fileName: AirPartitioner.java
 * @description: AirPartitioner.java类说明
 * @author: by echo huang
 * @date: 2020-03-26 16:25
 */
public class AirPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {

        return 0;
    }
}
