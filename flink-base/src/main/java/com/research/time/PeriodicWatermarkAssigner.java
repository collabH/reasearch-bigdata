package com.research.time;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class PeriodicWatermarkAssigner implements AssignerWithPeriodicWatermarks<String> {
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
    public long extractTimestamp(String element, long previousElementTimestamp) {
        long time = System.currentTimeMillis();
//        System.out.println("previousElementTimestamp" + previousElementTimestamp);
//        System.out.println("time" + time);
//        System.out.println("currentMaxTimestamp" + currentMaxTimestamp);
        //设置当前最大事件
        currentMaxTimestamp = Math.max(time, currentMaxTimestamp);
        return time;
    }
}
