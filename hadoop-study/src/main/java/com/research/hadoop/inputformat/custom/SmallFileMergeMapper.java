package com.research.hadoop.inputformat.custom;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: SmallFileMergeMapper.java
 * @description: SmallFileMergeMapper.java类说明
 * @author: by echo huang
 * @date: 2020-07-22 22:34
 */
public class SmallFileMergeMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
