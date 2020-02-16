/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.dataset;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * @fileName: ParameterPass.java
 * @description: 参数传递到函数内部
 * @author: by echo huang
 * @date: 2020-02-15 15:52
 */
public class ParameterPass {

    public static void main(String[] args) throws Exception {
        Configuration pa = new Configuration();
        pa.setString("h", "a");
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        //设置全局参数
        environment.getConfig().setGlobalJobParameters(pa);
        DataSource<String> dataSource = environment.fromElements("a", "b", "c");
        //构造方法传递参数
      /*  dataSource.filter(new MyFilter("a"))
                .print();*/
        //通过withParameters方法传递
        //  Configuration conf = new Configuration();
//        conf.setString("h", "a");
//        dataSource.withParameters(conf)
        dataSource
                .filter(new RichFilterFunction<String>() {
                    private String val;

                    @Override
                    public boolean filter(String s) throws Exception {
                        return val.equals(s);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                        Configuration globConf = (Configuration) globalParams;
                        val = globConf.getString("h", null);
                        //val = parameters.getString("h", "a");
                    }
                }).print();
    }


    @AllArgsConstructor
    @NoArgsConstructor
    static class MyFilter implements FilterFunction<String> {
        private String val;

        @Override
        public boolean filter(String s) throws Exception {
            return s.equals(val);
        }
    }
}
