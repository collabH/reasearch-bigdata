package org.telecome.common.contant;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.telecome.common.domain.Val;

/**
 * @fileName: Contants.java
 * @description: Contants.java类说明
 * @author: by echo huang
 * @date: 2020-08-10 22:37
 */
@AllArgsConstructor
public enum Contants implements Val<String> {
    NAMESPACE("ct");

    @Getter
    private String name;

    @Override
    public String value() {
        return NAMESPACE.name;
    }
}
