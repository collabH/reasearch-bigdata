package com.actual.batch;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

/**
 * @fileName: MapPartition.java
 * @description: MapPartition.java类说明
 * @author: by echo huang
 * @date: 2020-04-05 11:25
 */
public class MapPartition {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //产生数据
        DataSource<Long> dataSource = env.generateSequence(1, 20);

        dataSource.mapPartition(new MyMapPartition())
                .print();
    }

    static class MyMapPartition implements MapPartitionFunction<Long, Long> {

        @Override
        public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
            long sum = 0;
            for (Long value : values) {
                sum += value;
            }
            out.collect(sum);
        }
    }
}
