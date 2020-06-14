package dev.hive.test.inputformat;

import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @fileName: CustomSplit.java
 * @description: CustomSplit.java类说明
 * @author: by echo huang
 * @date: 2020-05-30 16:46
 */
public class CustomSplit implements InputSplit {
    @Override
    public long getLength() throws IOException {
        return 1;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[]{"localhost"};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
