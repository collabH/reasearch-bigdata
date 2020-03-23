/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.datasteam;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * @fileName: User.java
 * @description: User.java类说明
 * @author: by echo huang
 * @date: 2020-02-15 21:09
 */
@Data
@NoArgsConstructor
public class User {
    private Integer id;
    private String name;
    private Integer sex;

    public User(Integer id, String name, Integer sex) {
        this.id = id;
        this.name = name;
        this.sex = sex;
    }
    public Map<String, Object> map() {
        Map<String, Object> data = new HashMap<>();
        data.put("id", this.id);
        data.put("name", this.name);
        data.put("sex", this.sex);
        return data;
    }

}
