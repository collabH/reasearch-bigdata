/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research;

import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;

/**
 * @fileName: ApplicationContextManager.java
 * @description: guice容器
 * @author: by echo huang
 * @date: 2020-02-25 10:42
 */
@ThreadSafe
public class ApplicationContextManager {
    private static class InjectorHolder {
        private static List<AbstractModule> applicationContextList = Lists.newArrayList();
        private static Injector applicationContext;


        /**
         * 初始化guice容器环境
         */
        static {
            applicationContextList.add(new Test());
            applicationContextList.add(new Test2());
            applicationContext = Guice.createInjector(applicationContextList);
        }
    }

    private ApplicationContextManager() {
    }

    private static Injector getApplicationContext() {
        return InjectorHolder.applicationContext;
    }

    public static <T> T getInstance(Class<T> type) {
        return getApplicationContext().getInstance(type);
    }
}
