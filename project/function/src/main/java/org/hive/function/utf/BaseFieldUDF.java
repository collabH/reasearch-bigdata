package org.hive.function.utf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @fileName: BaseFieldUDF.java
 * @description: BaseFieldUDF.java类说明
 * @author: by echo huang
 * @date: 2020-08-18 23:07
 */
public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String jsonKeysString) {
        StringBuilder result = new StringBuilder();
        String[] jsonKeys = jsonKeysString.split(",");

        //服务器时间
        String[] logContents = line.split("\\|");
        if (logContents.length != 2 || StringUtils.isEmpty(logContents[1])) {
            return "";
        }

        String logContent = logContents[1];
        try {
            JSONObject jsonObject = new JSONObject(logContent);
            // 获取公共字段
            JSONObject cmJsonObject = jsonObject.getJSONObject("cm");

            for (String key : jsonKeys) {
                if (cmJsonObject.has(key.trim())) {
                    String value = cmJsonObject.getString(key.trim());
                    result.append(value).append("\t");
                } else {
                    result.append("\t");
                }
            }
            // 处理事件字段
            result.append(jsonObject.getString("et")).append("\t")
                    .append(logContents[0]).append("\t");
            return result.toString();
        } catch (JSONException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static void main(String[] args) {
        String json = "1597591116071|{\"cm\":{\"ln\":\"-94.4\",\"sv\":\"V2.0.0\",\"os\":\"8.0.4\",\"g\":\"HXDRWTXZ@gmail.com\",\"mid\":\"7\",\"nw\":\"WIFI\",\"l\":\"en\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"uid\":\"7\",\"t\":\"1597561354759\",\"la\":\"20.2\",\"md\":\"HTC-14\",\"vn\":\"1.2.9\",\"ba\":\"HTC\",\"sr\":\"M\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1597503084226\",\"en\":\"display\",\"kv\":{\"goodsid\":\"2\",\"action\":\"1\",\"extend1\":\"2\",\"place\":\"0\",\"category\":\"3\"}},{\"ett\":\"1597536024921\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"27\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"1\",\"type1\":\"\",\"loading_way\":\"2\"}},{\"ett\":\"1597496047698\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":3,\"addtime\":\"1597570796549\",\"praise_count\":578,\"other_id\":0,\"comment_id\":7,\"reply_count\":30,\"userid\":4,\"content\":\"粥蚊玫锗蛊阔挡衙挡屉互腿涧\"}},{\"ett\":\"1597544373472\",\"en\":\"favorites\",\"kv\":{\"course_id\":6,\"id\":0,\"add_time\":\"1597545194551\",\"userid\":1}}]}";

        BaseFieldUDF baseFieldUDF = new BaseFieldUDF();
        System.out.println(baseFieldUDF.evaluate(json, "ln,sv"));
    }
}
