package com.research.hadoop.join.reduce;

/**
 * @fileName: MetaDataParser.java
 * @description: MetaDataParser.java类说明
 * @author: by echo huang
 * @date: 2020-03-27 11:30
 */
public class MetaDataParser {
    private Integer id;

    private String name;

    private String temp;

    public String getTemp() {
        return temp;
    }

    public void setTemp(String temp) {
        this.temp = temp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void parser(String value) {
        String[] tokens = value.split(",");
        this.id = Integer.parseInt(tokens[0]);
        this.name = tokens[1];
    }

    public void parserTemperature(String value) {
        String[] tokens = value.split(",");
        this.temp = tokens[1];
    }
}
