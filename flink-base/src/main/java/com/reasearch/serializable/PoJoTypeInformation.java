package com.reasearch.serializable;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

/**
 * @fileName: PoJoTypeInformation.java
 * @description: 类型声明
 * @author: by echo huang
 * @date: 2020-03-11 16:08
 */
public class PoJoTypeInformation {

    public static void main(String[] args) {
        //非泛型类
        PojoTypeInfo<Person> typeInfo = (PojoTypeInfo<Person>) TypeInformation.of(Person.class);

        //泛型类
        TypeInformation<Tuple2<String, String>> resultType = TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        });
    }


    public class Person {
        private String id;
        private Integer age;


        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }
}
