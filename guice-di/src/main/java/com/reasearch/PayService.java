/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.reasearch;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.List;

/**
 * @fileName: PayService.java
 * @description: PayService.java类说明
 * @author: by echo huang
 * @date: 2020-02-25 10:26
 */
public class PayService {
    @Inject
    private List<Pay> pay;

    @Inject
    @Named(value = "pay1")
    private Pay pay1;

    public void pay() {
        pay.forEach(Pay::pay);
//        pay1.pay();
    }

}
