/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.nacos;

import lombok.Data;

/**
 * @fileName: ReRunProperties.java
 * @description: redis重跑机制
 * @author: by echo huang
 * @date: 2020-02-25 20:40
 */
@Data
public class ReRunProperties {
    private Boolean onOff;
    private Integer time;

}
