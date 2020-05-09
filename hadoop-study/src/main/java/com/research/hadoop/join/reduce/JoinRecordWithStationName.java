package com.research.hadoop.join.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @fileName: JoinRecordWithStationName.java
 * @description: JoinRecordWithStationName.java类说明
 * @author: by echo huang
 * @date: 2020-03-27 11:50
 */
public class JoinRecordWithStationName extends Partitioner<TextPair, Text> {
    @Override
    public int getPartition(TextPair textPair, Text text, int numPartitions) {
        return (textPair.getId().hashCode() & Integer.MAX_VALUE) % textPair.getId();
    }
}
