/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.reasearch.hadoop.mapreduce;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: MyMapper.java
 * @description: 读取输入文件
 * @author: by echo huang
 * @date: 2020-02-11 11:54
 * @see LongWritable,Text,Mapper
 */
@Slf4j
public class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //收到到的每一行数据按照指定分隔符进行拆分
        String line = value.toString();
        log.info("input line:{}", line);
        String[] words = StringUtils.split(line, " ");
        for (String word : words) {
            //通过上下文将map的结果输出
            context.write(new Text(word), one);
        }
    }
}
