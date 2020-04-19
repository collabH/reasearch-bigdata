package com.research.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class BookFacadeFactory implements InvocationHandler {


    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        Object result;
        System.out.println("事务开始...");
        result = method.invoke(proxy, args);
        System.out.println("事务结束...");

        return result;
    }

    public static void main(String[] args) throws Throwable {
        BookFacadeFactory proxy = new BookFacadeFactory();
        proxy.invoke(new Pay() {
            @Override
            public void pay() {
                System.out.println("test pay");
            }
        }, Pay.class.getMethod("pay", null), new Object[]{});

    }

}
