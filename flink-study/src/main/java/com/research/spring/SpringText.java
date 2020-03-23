/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @fileName: SpringText.java
 * @description: SpringText.java类说明
 * @author: by echo huang
 * @date: 2020-02-21 20:14
 */
@SpringBootApplication
public class SpringText {
    @Autowired
    private HelloImpl hello;
    public static void main(String[] args) {
        SpringApplication.run(SpringText.class, args);
        SpringText springText = new SpringText();
        springText.test();
    }

    private  void test(){
        System.out.println(hello);
    }

}
