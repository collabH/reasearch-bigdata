package com.research.hadoop.inputformat.custom;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @fileName: CustomInputFormat.java
 * @description: 用于合并小文件
 * @author: by echo huang
 * @date: 2020-07-22 21:47
 */
public class CustomInputFormat extends FileInputFormat<Text, BytesWritable> {

    /**
     * 不允许文件切割合并小文件
     *
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * 重写RecordReader，一次读取一个完整的文件封装到KV中
     *
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        CustomReader customReader = new CustomReader();
        customReader.initialize(inputSplit, taskAttemptContext);
        return customReader;
    }
}
