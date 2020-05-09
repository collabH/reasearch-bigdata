package com.research.cep;

import org.apache.flink.cep.pattern.Pattern;

/**
 * @fileName: Quantifier.java
 * @description: Quantifier.java类说明
 * @author: by echo huang
 * @date: 2020-04-04 15:34
 */
public class Quantifier {
    public static void main(String[] args) {
        Pattern<Object, Object> sd = Pattern.begin("sd")
                .times(4);
        System.out.println(sd);
    }
}
