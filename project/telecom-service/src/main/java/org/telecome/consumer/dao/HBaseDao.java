package org.telecome.consumer.dao;

import com.google.common.collect.Lists;
import org.telecome.common.domain.BaseDao;

/**
 * @fileName: HBaseDao.java
 * @description: HBaseDao.java类说明
 * @author: by echo huang
 * @date: 2020-08-11 23:21
 */
public class HBaseDao extends BaseDao {
    /**
     * 初始化
     */
    public void init() throws Exception {
        start();
        // 创建命名空间和表
        createNameSpace("ct");
        createTable("teamcom", Lists.newArrayList("caller"));
        stop();
    }

    /**
     * 插入数据
     *
     * @param value
     */
    public void insertData(String value) throws Exception {

    }
}
