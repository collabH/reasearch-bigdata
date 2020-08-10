package org.telecome.producer.domain;

import org.telecome.common.domain.Data;

/**
 * @fileName: Contact.java
 * @description: Contact.java类说明
 * @author: by echo huang
 * @date: 2020-08-10 23:37
 */
public class Contact extends Data {
    private String phone;
    private String name;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setValue(String value) {
        content = value;
        String[] arr = value.split("\t");
        this.phone = arr[0];
        this.name = arr[1];
    }
}
