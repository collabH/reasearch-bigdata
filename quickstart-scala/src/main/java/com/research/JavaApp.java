package com.research;

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
    }
}
