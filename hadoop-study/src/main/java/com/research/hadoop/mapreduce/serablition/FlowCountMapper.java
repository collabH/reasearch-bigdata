package com.research.hadoop.mapreduce.serablition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: FlowCountMapper.java
 * @description: FlowCountMapper.java类说明
 * @author: by echo huang
 * @date: 2020-07-19 16:19
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, Phone> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将value序列化为Phone

        String valueStr= value.toString();
        // private String phone;
        //    private String ip;
        //    private Integer upFlow;
        //    private Integer downFlow;
        //    private Integer sumFlow;
        //    private Integer status;
        // 1,15983211174,192.168.2.45,19,20,200
        String[] valueArr = valueStr.split(",");
        for (String val: valueArr) {

        }

    }
}
