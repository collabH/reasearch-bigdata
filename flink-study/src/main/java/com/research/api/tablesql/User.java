/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.tablesql;

import lombok.Data;

/**
 * @fileName: User.java
 * @description: User.java类说明
 * @author: by echo huang
 * @date: 2020-02-16 13:48
 */
@Data
public class User {
    public User(Integer id, String name, Integer price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    public User() {
    }

    private Integer id;
    private String name;
    private Integer price;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }
}
