package dev.hive.test.inputformat;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @fileName: CustomInputFormat.java
 * @description: CustomInputFormat.java类说明
 * @author: by echo huang
 * @date: 2020-05-30 16:46
 */
public class CustomInputFormat implements InputFormat {
    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        CustomSplit[] splits = new CustomSplit[1];
        splits[0] = new CustomSplit();
        return splits;
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new CustomRecordReader(jobConf, inputSplit);
    }
}
