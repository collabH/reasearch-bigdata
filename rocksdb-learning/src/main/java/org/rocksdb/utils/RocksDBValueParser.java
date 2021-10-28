package org.rocksdb.utils;

import com.google.common.collect.Lists;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @fileName: RocksDBValueParser.java
 * @description: RocksDBValueParser.java类说明
 * @author: huangshimin
 * @date: 2021/10/21 2:42 下午
 */
public class RocksDBValueParser implements Function<byte[], String> {

    @Override
    public String apply(byte[] values) {
        return new String(values, Charset.defaultCharset());
    }

    public static void main(String[] args) {
        List<Integer> potentialSubCustomerDataList = Lists.newArrayList(1, 23, 4, 535, 23, 213, 421, 1);
        List<List<Integer>> partitionData = Lists.partition(potentialSubCustomerDataList, 3);
        System.out.println(partitionData);
    }
}
