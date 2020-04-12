package com.research.hadoop.hive;

import org.apache.hadoop.hive.ql.udf.UDFChr;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * @fileName: UDFApp.java
 * @description: hive udf编写
 * @author: by echo huang
 * @date: 2020-04-11 21:15
 */
public class UDFApp extends UDFChr {
    @Override
    public Text evaluate(LongWritable n) {
        return super.evaluate(n);
    }

    @Override
    public Text evaluate(DoubleWritable n) {
        return super.evaluate(n);
    }
}
