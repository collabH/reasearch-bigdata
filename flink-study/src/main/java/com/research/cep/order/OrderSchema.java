package com.research.cep.order;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.Objects;

/**
 * @fileName: OrderSchema.java
 * @description: OrderSchema.java类说明
 * @author: by echo huang
 * @date: 2020-04-04 20:01
 */
public class OrderSchema implements DeserializationSchema<Order> {
    @Override
    public Order deserialize(byte[] message) throws IOException {
        String me = new String(message, Charsets.UTF_8);
        Gson gson = new Gson();
        return gson.fromJson(me, Order.class);
    }

    @Override
    public boolean isEndOfStream(Order nextElement) {
        return Objects.isNull(nextElement);
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}
