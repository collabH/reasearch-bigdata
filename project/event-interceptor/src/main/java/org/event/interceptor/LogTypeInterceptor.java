package org.event.interceptor;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * @fileName: LogTypeInterceptor.java
 * @description: Log启动拦截器
 * @author: by echo huang
 * @date: 2020-08-16 15:04
 */
public class LogTypeInterceptor implements Interceptor {

    private List<Event> addHeaderEvents = Lists.newArrayList();

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        String log = new String(event.getBody(), Charset.defaultCharset());

        // 设置header，后期选择器基于该绑定对应channel
        if (log.contains("start")) {
            headers.put("topic", "topic_start1");
        } else {
            headers.put("topic", "topic_event1");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        addHeaderEvents.clear();
        for (Event event : list) {
            addHeaderEvents.add(intercept(event));
        }
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
