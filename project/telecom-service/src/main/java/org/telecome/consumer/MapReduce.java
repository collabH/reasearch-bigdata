package org.telecome.consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * @fileName: MapReduce.java
 * @description: MapReduce.java类说明
 * @author: by echo huang
 * @date: 2020-08-13 23:43
 */
public class MapReduce extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("hbase mapreduce");
        job.setJarByClass(getClass());

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("caller"));
        // 设置mapper
        TableMapReduceUtil.initTableMapperJob("ct:teamcom", scan, HBaseMapper.class, Text.class, Text.class, job);

        // 设置reduce
        job.setReducerClass(HBaseReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置自定义OutputFormat
        job.setOutputFormatClass(MysqlOutputFormat.class);

        return job.waitForCompletion(true) ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new MapReduce(), args);
        System.exit(code);
    }
}

class HBaseMapper extends TableMapper<Text, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}

class HBaseReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}

class MysqlOutputFormat extends OutputFormat<Text, Text> {

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MysqlRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}

class MysqlRecordWriter extends RecordWriter<Text, Text> {

    @Override
    public void write(Text text, Text text2) throws IOException, InterruptedException {

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }
}