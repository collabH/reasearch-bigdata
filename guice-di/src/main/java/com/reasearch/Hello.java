package com.reasearch;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @fileName: Hello.java
 * @description: Hello.java类说明
 * @author: by echo huang
 * @date: 2020-03-12 00:22
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.PARAMETER})
@com.google.inject.ScopeAnnotation
public @interface Hello {
}
