/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.dataset;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;

/**
 * @fileName: ZippingElements.java
 * @description: 压缩元素
 * @author: by echo huang
 * @date: 2020-02-15 17:01
 */
public class ZippingElements {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        zipWithIndex(env);
        zipWithUniqueId(env);
    }

    private static void zipWithIndex(ExecutionEnvironment env) throws Exception {
        DataSource<String> data = env.fromElements("A", "B", "C", "D", "E", "F");
        DataSet<Tuple2<Long, String>> tuple2DataSet = DataSetUtils.zipWithIndex(data);
        tuple2DataSet.print();
    }


    private static void zipWithUniqueId(ExecutionEnvironment env) throws Exception {
        DataSource<String> data = env.fromElements("A", "B", "C", "D", "E", "F");
        DataSet<Tuple2<Long, String>> tuple2DataSet = DataSetUtils.zipWithUniqueId(data);
        tuple2DataSet.print();
    }
}
