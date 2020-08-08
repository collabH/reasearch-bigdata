package org.reasearch.hbase.api;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @fileName: HBaseMapReduce.java
 * @description: Hbase整合MapReduce
 * @author: by echo huang
 * @date: 2020-08-08 13:26
 */
public class HBaseMapReduce extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {

        return 0;
    }
}

class HBaseMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}

class HBaseReducer extends TableReducer<LongWritable, Text, NullWritable>{
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
