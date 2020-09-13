package com.research;

import com.research.concurrent.ActorModelDemo;
import com.research.traitdemo.Friend;

/**
 * @fileName: JavaApp.java
 * @description: JavaApp.java类说明
 * @author: by echo huang
 * @date: 2020-03-12 19:01
 */
public class JavaApp {

    private static ThreadLocal<String> threadLocal = ThreadLocal.withInitial(String::new);


    public static void main(String[] args) {
        threadLocal.set("hh");
        System.out.println(threadLocal.get());
        ActorModelDemo actorModelDemo = new ActorModelDemo();
    }
}

// 实现scala trait
class TestScala implements Friend {

    @Override
    public String name() {
        return null;
    }

    @Override
    public void listen() {

    }
}
