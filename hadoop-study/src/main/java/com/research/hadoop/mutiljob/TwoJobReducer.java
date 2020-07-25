package com.research.hadoop.mutiljob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @fileName: TwoJobReducer.java
 * @description: TwoJobReducer.java类说明
 * @author: by echo huang
 * @date: 2020-07-26 00:15
 */
public class TwoJobReducer extends Reducer<Text, Text, Text, Text> {
    private Text value = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder result = new StringBuilder(key.toString()).append("\t");
        for (Text value : values) {
            String[] fileds = value.toString().replace("--", "").split(" ");
            result.append(fileds[0]).append("-->").append(fileds[1]).append("\t");
        }
        value.set(result.toString());
        context.write(key, value);
    }
}
