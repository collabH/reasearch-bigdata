package com.research.proxy;

/**
 * @fileName: PayProxy.java
 * @description: PayProxy.java类说明
 * @author: by echo huang
 * @date: 2020-04-18 16:44
 */
public class PayProxy {
    Pay pay = new AppPay();

    public void operate() {
        System.out.println("handle 1");
        pay.pay();
        System.out.println("handle 2");
    }
}
