package org.research.flume.interceptor;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @fileName: TypeInterceptor.java
 * @description: 自定义Flume拦截器
 * @author: by echo huang
 * @date: 2020-07-31 00:12
 */
public class TypeInterceptor implements Interceptor {
    /**
     * 添加过头的事件
     */
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        this.addHeaderEvents = Lists.newArrayList();
    }

    /**
     * 单个事件拦截
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());
        //如果event的body包含hello则向header添加一个标签
        if (body.contains("hello")) {
            headers.put("type", "yes");
        } else {
            headers.put("type", "no");
        }
        return event;
    }

    /**
     * 批量事件拦截
     *
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        this.addHeaderEvents.clear();

        list.forEach(event -> addHeaderEvents.add(intercept(event)));

        return this.addHeaderEvents;
    }

    @Override
    public void close() {

    }

    public static class InterceptorBulder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        /**
         * 传递配置，可以将外部配置传递至内部
         *
         * @param context 配置上下文
         */
        @Override
        public void configure(Context context) {

        }
    }
}
