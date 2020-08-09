package org.reasearch.hbase.demo.infrastructure.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @fileName: HBaseConfig.java
 * @description: HBaseConfig.java类说明
 * @author: by echo huang
 * @date: 2020-08-09 11:03
 */
public class HBaseConfig {
    public static Configuration configuration = HBaseConfiguration.create();

    public static String nameSpace = "weibo";
    public static String contentTableCf = "info";
    public static String contentTable = "weibo:content";
    public static int contentVersion = 1;


    public static String relationTable = "weibo:relation";
    public static String relationTableCf1 = "attends";
    public static String relationTableCf2 = "fans";
    public static int relationVersion = 1;


    public static String inboxTable = "weibo:inbox";
    public static String inboxTableCF = "info";
    public static int inboxVersion = 2;

}
