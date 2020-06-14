package dev.hive.test.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Objects;

/**
 * @fileName: ETLMapper.java
 * @description: NullWritable 不会排序
 * @author: by echo huang
 * @date: 2020-06-14 13:56
 */
public class ETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取数据
        String oriStr = value.toString();

        //过滤数据
        String etlStr = ETLUtils.etlStr(oriStr);
        if (Objects.isNull(etlStr)) {
            return;
        }
        //写出
        v.set(etlStr);
        context.write(NullWritable.get(), v);
        //清空
        v.clear();
    }
}
