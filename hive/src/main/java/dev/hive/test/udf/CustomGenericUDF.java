package dev.hive.test.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * @fileName: CustomGenericUDF.java
 * @description: CustomGenericUDF.java类说明
 * @author: by echo huang
 * @date: 2020-05-27 23:51
 */
@Description(
        name = "nvl",
        extended = "example select nvl(null,'1') from test"
)
public class CustomGenericUDF extends GenericUDF {
    ObjectInspector[] args;

    GenericUDFUtils.ReturnObjectInspectorResolver returnObjectInspectorResolver;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        this.args = args;
        if (args.length != 2) {
            throw new UDFArgumentLengthException("NVL accpect 2 args");
        }
        //是否运行类型转换
        returnObjectInspectorResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        //
        if (!(returnObjectInspectorResolver.update(args[0]) && returnObjectInspectorResolver.update(args[1]))) {
            throw new UDFArgumentTypeException(2, "数据类型异常");
        }
        return returnObjectInspectorResolver.get();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object retVal = returnObjectInspectorResolver.convertIfNecessary(deferredObjects[0].get(), args[0]);
        //如果第一个参数
        if (retVal == null) {
            retVal = returnObjectInspectorResolver.convertIfNecessary(deferredObjects[1].get(), args[1]);
        }
        return retVal;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "if " +
                children[0] +
                " is null " +
                "returns" +
                children[1];
    }
}
