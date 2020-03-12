/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.nacos;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @fileName: NacosTest.java
 * @description: NacosTest.java类说明
 * @author: by echo huang
 * @date: 2020-02-25 18:45
 */
public class NacosTest {
    public static void main(String[] args) throws NacosException, InterruptedException {
        String serverAddr = "localhost";
        String dataId = "datasource-config";
        String group = "sandbox";
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.NAMESPACE, "c4351cf4-5c8f-4ca2-84bb-1665a17bdbd1");
        properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
        ConfigService configService = NacosFactory.createConfigService(properties);
        String content = configService.getConfig(dataId, group, 5000);
        DruidDataSource dataSource = JSON.parseObject(content, DruidDataSource.class);

        System.out.println(dataSource.getConnectProperties());
        configService.addListener(dataId, group, new Listener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                System.out.println("recieve:" + configInfo);
            }

            @Override
            public Executor getExecutor() {
                return null;
            }
        });
    }
}
