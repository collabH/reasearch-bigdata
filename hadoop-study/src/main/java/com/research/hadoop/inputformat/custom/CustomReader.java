package com.research.hadoop.inputformat.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @fileName: CustomReader.java
 * @description: CustomReader.java类说明
 * @author: by echo huang
 * @date: 2020-07-22 22:07
 */
public class CustomReader extends RecordReader<Text, BytesWritable> {

    // 文件分片，根据输入指定
    private FileSplit split = new FileSplit();
    private Text k = new Text();
    private Configuration configuration;
    private BytesWritable v = new BytesWritable();
    private boolean isProgress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();

    }

    /**
     * 将切片里的信息放入BytesWritable里
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress) {
            // 获取fs对象
            Path path = split.getPath();
            // 根据切片的路径拿到fs
            FileSystem fs = path.getFileSystem(configuration);
            // 拿到输入流
            FSDataInputStream inputStream = fs.open(path);

            byte[] buffer = new byte[(byte) split.getLength()];
            // 拷贝数据至缓存文件
            IOUtils.readFully(inputStream, buffer, 0, buffer.length);

            // 封装v
            v.set(buffer, 0, buffer.length);

            // 封装k
            k.set(path.toString());

            // 关闭资源
            IOUtils.closeStream(inputStream);

            isProgress = false;
            return true;
        }
        return false;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
