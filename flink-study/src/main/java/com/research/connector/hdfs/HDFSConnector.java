/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.connector.hdfs;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.concurrent.TimeUnit;

/**
 * @fileName: HDFSConnector.java
 * @description: HDFSConnector.java类说明
 * @author: by echo huang
 * @date: 2020-02-17 20:07
 */
public class HDFSConnector {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = environment.socketTextStream("localhost", 10011);
        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs:/streaming/file"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy( //滚动策略
                        DefaultRollingPolicy.create()//默认滚动策略
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(15))//15分钟的滚动间隔时间
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))//5分钟没有收到新的记录就关闭这个part
                                .withMaxPartSize(1024) //最大part文件大小 1kb
                                .build())
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HHmm"))
                .build();
        data.addSink(sink);

        //hdfs数据格式/base/hello/{date-time}/part-{parallel-task}-{count}
        environment.execute();
    }

    /**
     * Hadoop SequenceFile格式
     */

    private static void hadoopSequenceFile(DataStreamSource<Tuple2<LongWritable, Text>> data) {
        Configuration configuration = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
        StreamingFileSink<Tuple2<LongWritable, Text>> sink = StreamingFileSink.forBulkFormat(
                new Path("hdfs:/streaming/bulk"), new SequenceFileWriterFactory<>(configuration,
                        LongWritable.class, Text.class))
                .build();
        data.addSink(sink);
    }

    /**
     * Parquet格式
     */

    private void parquet() {
       /* Schema schema = ...;

        DataStream<GenericRecord> stream = ...;

        final StreamingFileSink<GenericRecord> sink = StreamingFileSink
                .forBulkFormat(outputBasePath, ParquetAvroWriters.forGenericRecord(schema))
                .build();

        stream.addSink(sink);*/
    }

}
