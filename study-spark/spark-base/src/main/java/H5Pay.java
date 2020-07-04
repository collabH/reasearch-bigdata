import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * @fileName: H5Pay.java
 * @description: H5Pay.java类说明
 * @author: by echo huang
 * @date: 2020-04-18 16:52
 */
public class H5Pay implements InvocationHandler {

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        TestProxy pay = (TestProxy) proxy;
        pay.test(String.valueOf(args[0]));
        return pay;
    }

    public static void main(String[] args) {

    }
}
