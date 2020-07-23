package com.research.hadoop.inputformat.custom;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @fileName: SmallFileMergeReducer.java
 * @description: SmallFileMergeReducer.java类说明
 * @author: by echo huang
 * @date: 2020-07-22 22:36
 */
public class SmallFileMergeReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        values.forEach(val -> {
            try {
                context.write(key, val);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
