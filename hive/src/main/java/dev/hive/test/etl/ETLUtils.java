package dev.hive.test.etl;

import java.util.Objects;

/**
 * @fileName: ETLUtils.java
 * @description: 原始数据ETL工具类，基于MR做的数据清洗
 * @author: by echo huang
 * @date: 2020-06-14 13:51
 */
public class ETLUtils {

    public static String etlStr(String oriStr) {
        StringBuffer buffer = new StringBuffer();
        if (Objects.isNull(oriStr)) {
            return null;
        }
        String[] oriArr = oriStr.split("\t");
        //字段少于9直接返回
        if (oriArr.length < 9) {
            return null;
        }
        //去掉空格
        oriArr[3] = oriArr[3].replaceAll(" ", "");
        for (int i = 0; i < oriArr.length; i++) {
            //对非相关ID进行处理
            if (i < 9) {
                //处理最后一个字段的分割符
                if (i == oriArr.length - 1) {
                    buffer.append(oriArr[i]);
                } else {
                    buffer.append(oriArr[i]).append("\t");
                }

            } else {
                //对相关id字段进行处理
                //处理最后一个字段的分割符
                if (i == oriArr.length - 1) {
                    buffer.append(oriArr[i]);
                } else {
                    buffer.append(oriArr[i]).append("&");
                }
            }
        }
        return buffer.toString();
    }
}
