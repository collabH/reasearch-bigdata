/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Provides;
import com.google.inject.name.Named;

import java.util.ArrayList;
import java.util.List;

/**
 * @fileName: Test.java
 * @description: Test.java类说明
 * @author: by echo huang
 * @date: 2020-02-25 10:17
 */
public class Test extends AbstractModule {
    @Override
    protected void configure() {
        //  bind(Pay.class).to(PayImpl.class);
    }

    @Provides
    public List<Pay> pay() {
        List<Pay> list = new ArrayList<>();
        list.add(new PayImpl());
        list.add(new PayImpl2());
        return list;
    }

    @Provides
    @Named(value = "pay1")
    public Pay pay1() {
        return new PayImpl();
    }

    @Provides
    @Named(value = "pay2")
    public Pay pay2() {
        return new PayImpl2();
    }

    @org.junit.Test
    public void  test(){
        Guice.createInjector(new Test()).getInstance(PayService.class).pay();
    }
}
