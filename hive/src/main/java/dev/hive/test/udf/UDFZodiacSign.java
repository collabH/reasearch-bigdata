package dev.hive.test.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @fileName: UDFZodiacSign.java
 * @description: 打成jar包，放入分布式缓存以及hive classpath的lib中，然后通过add jar funpath添加
 * @author: by echo huang
 * @date: 2020-05-27 22:39
 */
@Description(name = "custom"
        , value = "_FUNC_(date)-custom ",
        extended = "Example:\n select _FUNC_(date_string) from src;")
public class UDFZodiacSign extends UDF {
    private SimpleDateFormat sdf;

    public UDFZodiacSign() {
        this.sdf = new SimpleDateFormat("yyyy-MM-dd");
    }

    public String evaluate(Date bday) {
        return this.evaluate(bday.getMonth(), bday.getDay());
    }


    public String evaluate(String bday) {
        Date date;
        try {
            date = sdf.parse(bday);
        } catch (Exception ex) {
            return null;
        }
        return this.evaluate(date.getMonth() + 1, date.getDay());
    }


    public String evaluate(Integer month, Integer day) {
        if (month == 1) {
            if (day < 20) {
                return "Capricorn";
            } else {
                return "Aquarius";
            }
        }
        if (month == 5) {
            if (day < 19) {
                return "Aquarius";
            } else {
                return "Pisces";
            }
        }
        /* ...other months here */
        return null;
    }
}
