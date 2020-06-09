package com.research.hadoop.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * @fileName: StaticProxy.java
 * @description: StaticProxy.java类说明
 * @author: by echo huang
 * @date: 2020-06-09 18:36
 */
public class StaticProxy implements InvocationHandler {


    private Object pingpai;


    public StaticProxy(Object pingpai) {
        this.pingpai = pingpai;
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        // TODO Auto-generated method stub
        System.out.println("销售开始  柜台是： " + this.getClass().getSimpleName());
        System.out.println(method.getName());
        System.out.println("销售结束");
        return null;
    }

    public static void main(String[] args) throws NoSuchMethodException {
        StaticProxy staticProxy = new StaticProxy(new StaicProxyClassImpl());
    }
}
