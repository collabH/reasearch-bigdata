/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @fileName: TestVolatile.java
 * @description: TestVolatile.java类说明
 * @author: by echo huang
 * @date: 2020-02-15 19:29
 */
public class TestVolatile {
    private AtomicBoolean flag = new AtomicBoolean(true);

    public void start() {
        while (flag.get()) {
            System.out.println("start");
        }
    }

    public void clan(){
        flag.set(false);
    }

    public static void main(String[] args) throws InterruptedException {
        TestVolatile testVolatile = new TestVolatile();
        new Thread(()->{
            testVolatile.start();
        }).start();

        Thread.sleep(100);
        System.out.println("canle");
        new Thread(()->{
            testVolatile.clan();
        }).start();
    }
}
