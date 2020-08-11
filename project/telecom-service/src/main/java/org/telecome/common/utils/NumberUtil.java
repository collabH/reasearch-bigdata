package org.telecome.common.utils;

import java.text.DecimalFormat;

/**
 * @fileName: NumberUtil.java
 * @description: NumberUtil.java类说明
 * @author: by echo huang
 * @date: 2020-08-11 21:06
 */
public class NumberUtil {

    public static String fromat(int num, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append("0");
        }
        DecimalFormat df = new DecimalFormat(sb.toString());
        return df.format(num);
    }
}
