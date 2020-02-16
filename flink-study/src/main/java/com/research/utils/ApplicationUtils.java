/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @fileName: ApplicationUtils.java
 * @description: ApplicationUtils.java类说明
 * @author: by echo huang
 * @date: 2020-02-15 21:18
 */
@Component
public class ApplicationUtils implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

    public static <T> T getBean(String beanName, Class<T> clazz) {
        return applicationContext.getBean(beanName, clazz);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        ApplicationUtils.applicationContext = applicationContext;
    }
}
