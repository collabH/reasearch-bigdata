package com.research.hadoop.join.reduce;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

import static org.apache.commons.beanutils.BeanUtils.copyProperties;

/**
 * @fileName: TableReducer.java
 * @description: TableReducer.java类说明
 * @author: by echo huang
 * @date: 2020-07-25 13:36
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @SneakyThrows
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        List<TableBean> orderList = Lists.newArrayList();
        TableBean pbBean = new TableBean();
        for (TableBean order : values) {
            if ("order".equals(order.getFlag())) {
                TableBean orderBean = new TableBean();
                copyProperties(orderBean, order);
                orderList.add(orderBean);
            } else {
                copyProperties(pbBean, order);
            }
        }
        for (TableBean tableBean : orderList) {
            tableBean.setPName(pbBean.getPName());
            context.write(tableBean, NullWritable.get());
        }
    }
}
