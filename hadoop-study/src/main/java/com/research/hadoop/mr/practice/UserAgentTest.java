///*
// * Copyright: 2020 forchange Inc. All rights reserved.
// */
//
//package com.research.hadoop.mr.practice;
//
//import com.kumkee.userAgent.UserAgent;
//import com.kumkee.userAgent.UserAgentParser;
//import lombok.extern.slf4j.Slf4j;
//
///**
// * @fileName: UserAgentTest.java
// * @description: UserAgent测试类
// * @author: by echo huang
// * @date: 2020-02-12 10:36
// */
//@Slf4j
//public class UserAgentTest {
//    public static void main(String[] args) {
//        UserAgentParser userAgentParser = new UserAgentParser();
//        String source = "Mozilla/5.0 (compatible; Baiduspider/2.0;";
//        UserAgent agent = userAgentParser.parse(source);
//        System.out.println(agent.getBrowser() + agent.getEngine() + agent.getEngineVersion()
//                + agent.getOs() + agent.getPlatform() + agent.getVersion());
//    }
//}
