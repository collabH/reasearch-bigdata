package dev.hive.test.thrift;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @fileName: UseThriftHive.java
 * @description: UseThriftHive.java类说明
 * @author: by echo huang
 * @date: 2020-05-30 18:13
 */
public class UseThriftHive {

    public static void main(String[] args) throws SQLException {
        Connection con = DriverManager.getConnection("jdbc:hive2://hadoop:10000");
        Statement stmt = con.createStatement();
        ResultSet resultSet = stmt.executeQuery("select * from test2");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "---" + resultSet.getString(2));
        }
    }
}
