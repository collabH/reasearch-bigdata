package org.telecome.common.domain;

/**
 * @fileName: Data.java
 * @description: Data.java类说明
 * @author: by echo huang
 * @date: 2020-08-10 22:36
 */
public abstract class Data implements Val<String> {

    public String content;

    public void setValue(String value) {
        this.content = value;
    }

    @Override
    public String value() {
        return content;
    }
}
