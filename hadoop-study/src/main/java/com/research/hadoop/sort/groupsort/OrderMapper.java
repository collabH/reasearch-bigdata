package com.research.hadoop.sort.groupsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @fileName: OrderMapper.java
 * @description: OrderMapper.java类说明
 * @author: by echo huang
 * @date: 2020-07-24 22:37
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderDetail, NullWritable> {
    private OrderDetail orderDetail = new OrderDetail();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String order = value.toString();
        String[] orderArr = order.split(",");
        orderDetail.setId(Integer.valueOf(orderArr[0]));
        orderDetail.setProductName(orderArr[1]);
        orderDetail.setPrice(Double.valueOf(orderArr[2]));
        context.write(orderDetail, NullWritable.get());
    }
}
