package com.research.hadoop.outputformat.custom;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @fileName: FilterRecordWriter.java
 * @description: FilterRecordWriter.java类说明
 * @author: by echo huang
 * @date: 2020-07-25 11:18
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream fsMy;
    private FSDataOutputStream fsOther;

    public FilterRecordWriter(TaskAttemptContext context) throws IOException {
        // 获取文件系统
        FileSystem fs = FileSystem.get(context.getConfiguration());
        fsMy = fs.create(new Path("/Users/babywang/Desktop/output/my.log"));

        fsOther = fs.create(new Path("/Users/babywang/Desktop/output/other.log"));
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        boolean isExist = text.toString().contains("wy") || text.toString().contains("hsm");
        if (isExist) {
            fsMy.write(text.getBytes());
            return;
        }
        fsOther.write(text.getBytes());
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fsMy);
        IOUtils.closeStream(fsOther);
    }
}
