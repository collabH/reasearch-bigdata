package com.streming.kafka;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * @fileName: Test.java
 * @description: Test.java类说明
 * @author: by echo huang
 * @date: 2020-04-07 15:11
 */
public class Test {
    public static void main(String[] args) {
        System.out.println(new BigDecimal(10, MathContext.DECIMAL64).abs());
    }
}
