package org.event.interceptor;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.event.interceptor.utils.LogUtils;

import java.nio.charset.Charset;
import java.util.List;

/**
 * @fileName: LogEtlInterceptor.java
 * @description: LogEtlInterceptor.java类说明
 * @author: by echo huang
 * @date: 2020-08-16 15:26
 */
public class LogEtlInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String log = new String(event.getBody(), Charset.defaultCharset());

        // 启动日志
        if (log.contains("start")) {
            if (LogUtils.vilidateStart(log)) {
                return event;
            }
        } else {
            if (LogUtils.validateEvent(log)) {
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> events = Lists.newArrayList();
        for (Event event : list) {
            Event interceptEvent = intercept(event);
            if (interceptEvent != null) {
                events.add(event);
            }
        }
        return events;
    }

    @Override
    public void close() {

    }

    class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
