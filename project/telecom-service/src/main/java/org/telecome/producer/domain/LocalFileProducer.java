package org.telecome.producer.domain;

import org.telecome.common.domain.DataIn;
import org.telecome.common.domain.DataOut;
import org.telecome.common.domain.DataProducer;

import java.io.IOException;
import java.util.List;

/**
 * @fileName: LocalFileProducer.java
 * @description: 本地数据文件生产者
 * @author: by echo huang
 * @date: 2020-08-10 22:42
 */
public class LocalFileProducer implements DataProducer {

    private DataIn in;
    private DataOut out;

    @Override
    public void setIn(DataIn dataIn) {
        this.in = dataIn;
    }

    @Override
    public void setOut(DataOut dataOut) {
        this.out = dataOut;
    }

    @Override
    public void produce() {
        try {
            List<Contact> read = in.read(Contact.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws IOException {

    }
}
