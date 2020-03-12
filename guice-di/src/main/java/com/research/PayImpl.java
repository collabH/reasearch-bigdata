/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research;

import com.google.inject.Singleton;

/**
 * @fileName: PayImpl.java
 * @description: PayImpl.java类说明
 * @author: by echo huang
 * @date: 2020-02-25 10:15
 */
@Singleton
public class PayImpl implements Pay {
    public void pay() {
        System.out.println("pay");
    }
}
