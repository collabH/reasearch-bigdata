package org.rocksdb.utils;

import java.nio.charset.Charset;
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
}
