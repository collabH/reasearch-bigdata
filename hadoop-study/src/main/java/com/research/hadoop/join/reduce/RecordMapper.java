package com.research.hadoop.join.reduce;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: StationMapper.java
 * @description: StationMapper.java类说明
 * @author: by echo huang
 * @date: 2020-03-27 11:28
 */
public class RecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private MetaDataParser parser = new MetaDataParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        this.parser.parser(new String(value.getBytes(), Charsets.UTF_8));
        this.parser.parserTemperature(new String(value.getBytes(), Charsets.UTF_8));
        context.write(new TextPair(parser.getId(), "1"), new Text(parser.getTemp()));
    }
}