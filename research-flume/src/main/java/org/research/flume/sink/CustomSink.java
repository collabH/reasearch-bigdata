package org.research.flume.sink;

import com.google.common.base.Charsets;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @fileName: CustomSink.java
 * @description: 自定义Sink
 * @author: by echo huang
 * @date: 2020-08-01 15:39
 */
public class CustomSink extends AbstractSink implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomSink.class);

    private String prefix;
    private String suffix;

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel channel = getChannel();
        //拿到channel事务
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event take = channel.take();

            String body = new String(take.getBody(), Charsets.UTF_8);
            LOGGER.info("result:{}", prefix + body + suffix);
            transaction.commit();
            status = Status.READY;
        } catch (Exception e) {
            transaction.rollback();
            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override
    public void configure(Context context) {
        this.prefix = context.getString("prefix");
        this.suffix = context.getString("suffix", "on the road");
    }
}
