package com.research.hadoop.join.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @fileName: JoinReducer.java
 * @description: JoinReducer.java类说明
 * @author: by echo huang
 * @date: 2020-03-27 11:45
 */
public class JoinReducer extends Reducer<TextPair, Text, IntWritable, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        Text stationName = new Text(iterator.next());
        while (iterator.hasNext()) {
            Text record = iterator.next();
            Text outValue = new Text(stationName.toString() + "\t" + record);
            context.write(new IntWritable(key.getId()), outValue);
        }

    }
}
