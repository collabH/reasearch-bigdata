package dev.hive.test.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @fileName: MyUDTF.java
 * @description: MyUDTF.java类说明
 * @author: by echo huang
 * @date: 2020-06-08 22:28
 */
public class MyUDTF extends GenericUDTF {

    private List<String> dataList = new ArrayList<>();

    /**
     * 定义输出数据的列名和数据类型
     *
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        /**
         * fieldNames:输出数据的列名
         * fieldOIs:输出数据的类型
         */
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        //函数的返回值
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 具体的处理逻辑
     *
     * @param args 接收的参数
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {

        if (Objects.isNull(args) || args.length != 2) {
            throw new UDFArgumentLengthException("输入参数异常");
        }

        //1.获取数据
        String data = String.valueOf(args[0]);

        //2.获取分隔符
        String splitKey = String.valueOf(args[1]);

        //3.切分数据
        String[] words = data.split(splitKey);

        //4.写出
        for (String word : words) {
            dataList.clear();
            //5.将数据放入集合
            dataList.add(word);
            //输出数据到收集器 collector，类似于MR中context的环形缓冲区
            forward(dataList);
        }

    }

    /**
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {

    }
}
