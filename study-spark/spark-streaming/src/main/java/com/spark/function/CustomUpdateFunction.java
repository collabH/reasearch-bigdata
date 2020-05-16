package com.spark.function;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

/**
 * @fileName: CustomUpdateFunction.java
 * @description: CustomUpdateFunction.java类说明
 * @author: by echo huang
 * @date: 2020-04-25 16:26
 */
public class CustomUpdateFunction implements Function2<List<Integer>, Optional<Integer>, Optional<Integer>> {
    @Override
    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
        Integer newSum = values.stream()
                .reduce((v1, v2) -> v1 + v1)
                .orElse(0) + state.or(0);
        return Optional.of(newSum);
    }
}
