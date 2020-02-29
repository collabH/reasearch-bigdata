/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.reasearch;

import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

/**
 * @fileName: Test2.java
 * @description: Test2.java类说明
 * @author: by echo huang
 * @date: 2020-02-25 17:14
 */
public class Test2 extends AbstractModule {

    public static void main(String[] args) {

    }

    private Injector injector;

    @Before
    public void evn() {
        ArrayList<AbstractModule> abstractModules = Lists.newArrayList(new Test2(), new com.reasearch.Test());
        injector = Guice.createInjector(abstractModules);
    }


    @Test
    public void test() {
        injector.getInstance(Key.get(Pay.class, Names.named("pay2"))).pay();
    }
}
