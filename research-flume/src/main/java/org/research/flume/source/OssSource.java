package org.research.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * @fileName: OssSource.java
 * @description: 配置OssSource
 * @author: by echo huang
 * @date: 2020-08-01 00:12
 */
public class OssSource extends AbstractSource implements Configurable, PollableSource {

    private String prefix;
    private String suffix;

    @Override
    public void configure(Context context) {
        this.prefix = context.getString("prefix");
        this.suffix = context.getString("suffix", "suffix");

    }

    /**
     * 1.接受数据(读取OSS数据)
     * 2.封装事件
     * 3.将事件传递给Channel
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        try {
            for (int i = 0; i < 5; i++) {
                SimpleEvent event = new SimpleEvent();
                event.setBody((prefix + "--" + i + "--" + suffix).getBytes());
                //传递数据给Channel
                getChannelProcessor().processEvent(event);
                status = Status.READY;
            }
            Thread.sleep(2000);
        } catch (Exception e) {
            status = Status.BACKOFF;
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
