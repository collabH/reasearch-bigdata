package org.telecome.producer.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @fileName: Calllog.java
 * @description: Calllog.java类说明
 * @author: by echo huang
 * @date: 2020-08-11 21:23
 */
@Data
@AllArgsConstructor
@Builder
@NoArgsConstructor
public class Calllog {
    private String call1;
    private String call2;
    private String calltime;
    private String duration;

    @Override
    public String toString() {
        return call1 + "\t" + call2 + "\t" + calltime + "\t" + duration;
    }
}
