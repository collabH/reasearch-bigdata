package com.research.hadoop.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @fileName: ThreadPoolAfter.java
 * @description: ThreadPoolAfter.java类说明
 * @author: by echo huang
 * @date: 2020-03-26 10:22
 */
public class ThreadPoolAfter {
    private static final ExecutorService EXECUTOR= Executors.newSingleThreadExecutor();

    public static void main(String[] args) {
        System.out.println((byte)"\t".charAt(0));
    }
}
