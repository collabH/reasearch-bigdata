package com.research.cep;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @fileName: Watermarks.java
 * @description: Watermarks.java类说明
 * @author: by echo huang
 * @date: 2020-04-03 11:37
 */
public class Watermarks implements AssignerWithPeriodicWatermarks<Event> {
    /**
     * 最大延迟时间
     */
    private final long maxOutOfOrderness = 350000;

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
    public long extractTimestamp(Event element, long previousElementTimestamp) {
        long time = System.currentTimeMillis();
        currentMaxTimestamp = Math.max(time, currentMaxTimestamp);
        return time;
    }
}
