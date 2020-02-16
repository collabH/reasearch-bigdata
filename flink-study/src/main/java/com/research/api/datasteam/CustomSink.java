/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.datasteam;

import com.research.utils.ApplicationUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * @fileName: CustomSink.java
 * @description: CustomSink.java类说明
 * @author: by echo huang
 * @date: 2020-02-15 21:09
 */
@SpringBootApplication(scanBasePackages = "com.research")
@EnableAsync
@RestController
public class CustomSink {

    public static void main(String[] args) throws Exception {


        SpringApplication.run(CustomSink.class, args);

    }

    @GetMapping(value = "start")
    public void start1() throws Exception {
        start();
    }

    @Async
    public void start() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        data.flatMap(new FlatMapFunction<String, User>() {
            @Override
            public void flatMap(String val, Collector<User> collector) throws Exception {
                String[] tokens = val.split(",");
                if (tokens.length < 2) {
                    throw new RuntimeException("参数异常");
                }
                collector.collect(new User(2, tokens[0], 1));
            }
        }).addSink(new MysqlRichSink()).setParallelism(2);
        JobExecutionResult result = env.execute("save mysql");
        System.out.println("耗时:" + result.getNetRuntime());
    }

    static class MysqlSink implements SinkFunction<User> {
        @Override
        public void invoke(User value, Context context) throws Exception {

            NamedParameterJdbcTemplate jdbc = ApplicationUtils.getBean("jdbc", NamedParameterJdbcTemplate.class);
            Map<String, Object> map = value.map();
            jdbc.update("insert  into user0(id,name,sex) values(:id,:name,:sex)", map);
        }
    }

    /**
     * 富sink函数
     */
    static class MysqlRichSink extends RichSinkFunction<User> {
        private NamedParameterJdbcTemplate jdbc;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            jdbc = ApplicationUtils.getBean("jdbc", NamedParameterJdbcTemplate.class);
        }

        @Override
        public void invoke(User value, Context context) throws Exception {
            Map<String, Object> map = value.map();
            jdbc.update("insert  into user0(id,name,sex) values(:id,:name,:sex)", map);
        }
    }
}
