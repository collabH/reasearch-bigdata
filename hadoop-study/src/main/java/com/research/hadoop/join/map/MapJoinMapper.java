package com.research.hadoop.join.map;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

/**
 * @fileName: MapJoinMapper.java
 * @description: MapJoinMapper.java类说明
 * @author: by echo huang
 * @date: 2020-07-25 14:32
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> pdMap = Maps.newHashMap();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), Charsets.UTF_8));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] pdArr = line.split(",");
            pdMap.put(pdArr[0], pdArr[1]);

        }
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] orderArr = value.toString().split(",");
        String pdName = pdMap.getOrDefault(orderArr[1], "");
        String id = orderArr[0];
        String amount = orderArr[2];
        String line = id + "\t" + pdName + "\t" + amount;
        k.set(line);
        context.write(k, NullWritable.get());
    }
}
