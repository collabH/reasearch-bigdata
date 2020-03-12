package com.research.api.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @fileName: QueryTable.java
 * @description: QueryTable.java类说明
 * @author: by echo huang
 * @date: 2020-03-09 18:06
 */
public class QueryTable {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env);
        Table orders = batchTableEnvironment.scan("Orders");

        Table result = orders.filter("cCountry==='FRANCE'")
                .groupBy("cID,cName")
                .select("cID,cName,revenue.sum AS revSum");

    }
}
