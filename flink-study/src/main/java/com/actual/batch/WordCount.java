package com.actual.batch;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @fileName: WordCount.java
 * @description: WordCount.java类说明
 * @author: by echo huang
 * @date: 2020-04-05 10:48
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> data = Lists.newArrayList("huangsm,shuaiqi,haha,oo,lfy,zzl,wy,zz", "zzl,wy,zz,hello", "oo");
        DataSource<String> dataSource = env.fromCollection(data);

        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] token = value.split(",");
                for (String s : token) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        }).groupBy("f0")
                .sum(1)
                .print();
    }
}
