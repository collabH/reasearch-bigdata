/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.config;

import com.mysql.jdbc.Driver;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

/**
 * @fileName: DataSourceConfig.java
 * @description: DataSourceConfig.java类说明
 * @author: by echo huang
 * @date: 2020-02-15 21:33
 */
@Configuration
public class DataSourceConfig {
    @Bean
    public DataSource dataSource() {
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setUsername("root");
        hikariDataSource.setPassword("root");
        hikariDataSource.setJdbcUrl("jdbc:mysql://localhost:3306/ds0");
        hikariDataSource.setDriverClassName(Driver.class.getName());
        return hikariDataSource;
    }

    @Bean(name = "jdbc")
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate() {
        return new NamedParameterJdbcTemplate(dataSource());
    }
}
