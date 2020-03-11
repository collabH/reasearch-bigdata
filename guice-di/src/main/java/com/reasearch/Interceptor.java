package com.reasearch;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import java.lang.reflect.InvocationTargetException;

/**
 * @fileName: Interceptor.java
 * @description: Interceptor.java类说明
 * @author: by echo huang
 * @date: 2020-03-12 00:23
 */
public class Interceptor implements MethodInterceptor {
    public Object invoke(MethodInvocation methodInvocation) throws Throwable {
        return methodInvocation.proceed();
    }

    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<Interceptor> interceptorClass = Interceptor.class;
        Object invoke = interceptorClass.getMethod("invoke", MethodInvocation.class).invoke("zhangsan", "lisi");
        System.out.println(invoke);
    }
}
