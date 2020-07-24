package com.research.hadoop.sort.groupsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @fileName: OrderReducer.java
 * @description: OrderReducer.java类说明
 * @author: by echo huang
 * @date: 2020-07-24 22:39
 */
public class OrderReducer extends Reducer<OrderDetail, NullWritable, OrderDetail, NullWritable> {
    @Override
    protected void reduce(OrderDetail key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
