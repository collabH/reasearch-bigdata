package com.research.training;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @fileName: PeriodicWatermarkAssigner.java
 * @description: PeriodicWatermarkAssigner.java类说明
 * @author: by echo huang
 * @date: 2020-02-29 21:09
 */
public class PeriodicWatermarkAssigner implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, Long>> {
    /**
     * 最大延迟时间
     */
    private final long maxOutOfOrderness = 3500;

    /**
     * 当前最大时间
     */
    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        //获得当前水印为当前最大时间减去最大延迟时间
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }


    /**
     * 抽取timestamp
     *
     * @param element
     * @param previousElementTimestamp
     * @return
     */
    @Override
    public long extractTimestamp(Tuple3<Long, String, Long> element, long previousElementTimestamp) {
        long time = element.f0;
//        System.out.println("previousElementTimestamp" + previousElementTimestamp);
//        System.out.println("time" + time);
//        System.out.println("currentMaxTimestamp" + currentMaxTimestamp);
        //设置当前最大事件
        currentMaxTimestamp = Math.max(time, currentMaxTimestamp);
        return time;
    }
}
