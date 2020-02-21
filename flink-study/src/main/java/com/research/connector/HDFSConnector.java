/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.connector;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

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
                                .build()).build();
        data.addSink(sink);


        //hdfs数据格式/base/hello/{date-time}/part-{parallel-task}-{count}
        environment.execute();
    }
}
