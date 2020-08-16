package org.event.interceptor.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * @fileName: LogUtils.java
 * @description: LogUtils.java类说明
 * @author: by echo huang
 * @date: 2020-08-16 15:40
 */
public class LogUtils {

    public static boolean vilidateStart(String log) {
        if (StringUtils.isEmpty(log))
            return false;
        return StringUtils.startsWith(log.trim(), "{") && StringUtils.endsWith(log.trim(), "}");
    }


    public static boolean validateEvent(String log) {
        if (StringUtils.isEmpty(log))
            return false;

        String[] logArr = StringUtils.split(log, "\\|");
        if (logArr.length != 2) {
            return false;
        }
        // 第一位为服务器时间，毫秒时间戳
        if (logArr[0].length() != 13 || !NumberUtils.isDigits(logArr[0])) {
            return false;
        }

        return StringUtils.startsWith(logArr[1].trim(), "{") &&
                StringUtils.endsWith(logArr[1].trim(), "}");
    }
}
