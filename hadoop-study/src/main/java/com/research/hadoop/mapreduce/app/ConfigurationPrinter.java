package com.research.hadoop.mapreduce.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * @fileName: ConfigurationPrinter.java
 * @description: 读取配置
 * @author: by echo huang
 * @date: 2020-03-22 15:15
 */
public class ConfigurationPrinter extends Configured implements Tool {

    static {

        //加载默认配置
        // adds the default resources
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("configuration.xml");
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int count = ToolRunner.run(new ConfigurationPrinter(), args);
        System.exit(count);
    }
}
