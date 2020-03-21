package com.research.hadoop.mapreduce.app;

import org.apache.hadoop.conf.Configuration;
import org.hamcrest.Matchers;
import org.junit.Assert;

/**
 * @fileName: ConfApp.java
 * @description: ConfApp.java类说明
 * @author: by echo huang
 * @date: 2020-03-21 18:19
 */
public class ConfApp {

    public static void main(String[] args) {
//        readConf();
        merge();
    }

    /**
     * 读取配置
     */
    public static void readConf() {
        Configuration conf = new Configuration();
        conf.addResource("configuration.xml");

        Assert.assertThat(conf.get("color"), Matchers.is("red"));
        Assert.assertThat(conf.get("size"), Matchers.is("10"));
        Assert.assertThat(conf.get("size-weight"), Matchers.is("10,heavy"));
    }

    /**
     * 配置合并
     * 设置   <final>true</final>无法覆盖
     */
    public static void merge() {

        Configuration conf = new Configuration();
        conf.addResource("configuration.xml");
        conf.addResource("configuration1.xml");
        System.out.println(conf.get("size"));

        System.out.println(conf.get("weight"));

    }
}
