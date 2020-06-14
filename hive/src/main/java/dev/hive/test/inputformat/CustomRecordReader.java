package dev.hive.test.inputformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * @fileName: CustomRecordReader.java
 * @description: CustomRecordReader.java类说明
 * @author: by echo huang
 * @date: 2020-05-30 16:48
 */
public class CustomRecordReader implements RecordReader<Text, Text> {
    boolean hasNext = true;

    public CustomRecordReader(JobConf jobConf, InputSplit inputSplit) {

    }


    @Override
    public boolean next(Text text, Text text2) throws IOException {
        if (hasNext) {
            hasNext = false;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Text createKey() {
        return new Text("");
    }

    @Override
    public Text createValue() {
        return new Text("");
    }


    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
        if (hasNext) {
            return 0.0f;
        } else
            return 1.0f;
    }
}
