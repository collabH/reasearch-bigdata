package com.research.hadoop.hive;

import org.apache.hadoop.util.ReflectionUtils;

/**
 * @fileName: UDAFApp.java
 * @description: UDAFApp.java类说明
 * @author: by echo huang
 * @date: 2020-04-12 00:47
 */
public class UDAFApp {
    private void say(String name) throws Exception{
        System.out.println(name);
    }

    public static void main(String[] args) throws Exception {
        UDAFApp udafApp = ReflectionUtils.newInstance(UDAFApp.class, null);
        udafApp.say("tt");
    }

}
