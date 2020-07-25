/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.hadoop.mr.parititioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @fileName: MyPartitioner.java
 * @description: MyPartitioner.java类说明
 * @author: by echo huang
 * @date: 2020-02-11 17:58
 */
public class MyPartitioner extends Partitioner<Text, LongWritable> {

    /**
     * 选择到对应的reducer-task上执行
     *
     * @param text
     * @param longWritable
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        //这里规定的文本是iphone、xiaomi、huawei、nokia
        if ("xiaomi".equals(text.toString())) {
            return 0;
        }
        if ("iphone".equals(text.toString())) {
            return 1;
        }
        if ("huawei".equals(text.toString())) {
            return 2;
        }
        return 3;
    }
}
