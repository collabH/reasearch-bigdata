package com.research.cep;

import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * @fileName: CombiningPatterns.java
 * @description: 组合模式
 * @author: by echo huang
 * @date: 2020-04-03 17:27
 */
public class CombiningPatterns {
    public static void main(String[] args) {
        Pattern<String, String> start = Pattern.<String>begin("start");


        //严格连续性
        start.next("xx");

        //宽松连续性
        start.followedBy("xx");

        //非确定宽松连续性
        start.followedByAny("xx");

        //如果不是想某个事件跟随另一个事件
        start.notNext("xx");

        //如果不希望事件类型介于俩个事件类型之间

        //模式序列不能以notFollowedBy结尾
        start.notFollowedBy("xx");

        /**
         * 如果"a b"给定事件序列的pattern "a", "c", "b1", "b2"将给出以下结果：
         * 1.严格连续性:模式给的事件不匹配。"c"后面的"a"将被丢弃
         * 2.宽松连续性: "a b"将匹配:{"a","b1"},因为宽松的连续性将“跳过不匹配的事件，直到下一个匹配的事件”。
         * 3.非确定宽松连续性:"a b"将匹配:{"a","b1"},{"a","b2"}
         */

        //GroupPattern


        GroupPattern<Event, Event> groupPattern = Pattern.begin(
                Pattern.<Event>begin("start")
                        .where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return false;
                            }
                        }).followedBy("start_middle").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return false;
                    }
                })
        );

        GroupPattern<Event, Event> strict = groupPattern.next(Pattern.<Event>begin("next_start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return false;
                    }
                }).followedBy("next_middle").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return false;
                    }
                }));

        // relaxed contiguity 宽松连续形式
        Pattern<Event, ?> relaxed = groupPattern.followedBy(
                Pattern.<Event>begin("followedby_start").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return false;
                    }
                }).followedBy("followedby_middle").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return false;
                    }
                })).oneOrMore();

        // non-deterministic relaxed contiguity不确定的宽松的连续形式
        Pattern<Event, ?> nonDetermin = groupPattern.followedByAny(
                Pattern.<Event>begin("followedbyany_start").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return false;
                    }
                }).followedBy("followedbyany_middle").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return false;
                    }
                })).optional();
    }
}
