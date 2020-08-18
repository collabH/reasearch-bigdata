package org.hive.function.udtf;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.List;

/**
 * @fileName: EventJsonUDTF.java
 * @description: EventJsonUDTF.java类说明
 * @author: by echo huang
 * @date: 2020-08-18 23:21
 */
public class EventJsonUDTF extends GenericUDTF {


    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        // 定义输出的变量的名称和类型
        List<String> filedNames = Lists.newArrayList();
        List<ObjectInspector> fieldsType = Lists.newArrayList();
        filedNames.add("event_name");
        fieldsType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        filedNames.add("event_json");
        fieldsType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(filedNames, fieldsType);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // 输入的值
        String input = String.valueOf(objects[0]);
        if (StringUtils.isEmpty(input)) {
            return;
        }

        try {
            JSONArray jsonArray = new JSONArray(input);
            for (int i = 0; i < jsonArray.length(); i++) {
                String[] results = new String[2];
                results[0] = jsonArray.getJSONObject(i).getString("en");
                results[1] = jsonArray.getString(i);
                // 类似与mr的环形缓冲区
                forward(results);
            }
        } catch (JSONException e) {
            e.printStackTrace();
            return;
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
