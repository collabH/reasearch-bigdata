/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.datasteam;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @fileName: DataSource.java
 * @description: 创建dataSource demo
 * @author: by echo huang
 * @date: 2020-02-15 18:45
 */
public class DataSource {
    private static final String filePath = "file:///Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/format";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      /*  //文件中读取
        env.readTextFile(filePath).print();
        //内存元素读取
        env.fromElements(1, 2, 3).print();
        //内存集合读取
        env.fromCollection(Lists.newArrayList(1, 2, 3, 4)).print();
        //socket读取
        env.socketTextStream("localhost", 10001).print();
        //并行集合输出
        env.fromParallelCollection(new SplittableIterator<Integer>() {
            @Override
            public Iterator<Integer>[] split(int i) {
                return new Iterator[0];
            }

            @Override
            public int getMaximumNumberOfSplits() {
                return 0;
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Integer next() {
                return null;
            }
        }, Integer.class);*/

        customSource(env);
        env.execute();
    }

    /**
     * 自定义source
     *
     * @param env
     * @throws Exception
     */
    private static void customSource(StreamExecutionEnvironment env) throws Exception {

        //自定义非并行source源
//        env.addSource(new CustomSourceFunction())
        //无法设置并行度
        //.setParallelism(2)

        //自定义并行source
        env.addSource(new CustomParallelSourceFunction())
                //.setParallelism(4)
                //自定义富并行source
//        env.addSource(new CustomParallelRichSourceFunction())
                .setParallelism(4)
                .print();


    }

    /**
     * 不能并行的自定义source
     */
    static class CustomSourceFunction implements SourceFunction<Long> {
        private long val;
        private AtomicBoolean isRunning = new AtomicBoolean(true);

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning.get() && val < 100) {
                ctx.collect(val);
                val++;
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning.compareAndSet(true, false);
        }
    }

    /**
     * 支持并行的自定义Source
     */
    static class CustomParallelSourceFunction implements ParallelSourceFunction<Long> {

        private long val;
        private AtomicBoolean isRunning = new AtomicBoolean(true);

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning.get() && val < 100) {
                ctx.collect(val);
                val++;
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning.compareAndSet(true, false);
        }
    }

    /**
     * 自定义富并行数据源
     */
    static class CustomParallelRichSourceFunction extends RichParallelSourceFunction<Long> {

        private long val;
        private AtomicBoolean isRunning = new AtomicBoolean(true);

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning.get() && val < 100) {
                ctx.collect(val);
                val++;
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning.compareAndSet(true, false);
        }

        @Override
        public void setRuntimeContext(RuntimeContext t) {
            super.setRuntimeContext(t);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return super.getRuntimeContext();
        }

        @Override
        public IterationRuntimeContext getIterationRuntimeContext() {
            return super.getIterationRuntimeContext();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
