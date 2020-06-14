package dev.hive.test.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * @fileName: ParseBookUDF.java
 * @description: ParseBookUDF.java类说明
 * @author: by echo huang
 * @date: 2020-05-29 11:08
 */
@Description(name = "parse_arg",
        extended = "select parse_arg(name) from tableName;")
public class ParseBookUDF extends GenericUDTF {
    private Text text;
    Object[] forwardObj = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory
                .STRING));

        fieldNames.add("desc");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory
                .STRING));

        fieldNames.add("arr");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING)));
        forwardObj = new Object[3];
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
    }

    @Override
    public void close() throws HiveException {

    }
}
