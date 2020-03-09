package com.research;

/**
 * @fileName: TestRecursion.java
 * @description: TestRecursion.java类说明
 * @author: by echo huang
 * @date: 2020-03-06 18:24
 */
public class TestRecursion {

    public static void main(String[] args) {
        say();
    }
    private static void say(){
        while (true){
            say();
        }
    }
}
