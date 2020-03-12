/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.nacos;

import lombok.Data;

/**
 * @fileName: KafkaProperties.java
 * @description: kafka配置
 * @author: by echo huang
 * @date: 2020-02-17 18:47
 */

@Data
public class KafkaProperties {
    private String topic;
    private String bootstrapServers;
    private String groupId;
    private Integer maxPollRecords;

}
