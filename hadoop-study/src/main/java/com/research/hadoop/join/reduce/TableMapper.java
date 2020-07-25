package com.research.hadoop.join.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @fileName: TableMapper.java
 * @description: TableMapper.java类说明
 * @author: by echo huang
 * @date: 2020-07-25 11:54
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private String fileName;
    private TableBean tableBean = new TableBean();
    private Text k = new Text();

    @Override

    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (fileName.startsWith("order")) {
            String[] split = line.split(",");
            tableBean.setId(split[0]);
            tableBean.setPid(split[1]);
            tableBean.setAmount(Integer.parseInt(split[2]));
            tableBean.setPName("");
            tableBean.setFlag("order");
            k.set(split[1]);
        } else {
            String[] split = line.split(",");
            tableBean.setId("");
            tableBean.setAmount(0);
            tableBean.setPid(split[0]);
            tableBean.setPName(split[1]);
            tableBean.setFlag("pd");
            k.set(split[0]);
        }
        context.write(k, tableBean);
    }
}
