package org.telecome.common.domain;

import java.io.Closeable;

/**
 * @fileName: DataProducer.java
 * @description: 数据生产者
 * @author: by echo huang
 * @date: 2020-08-10 22:13
 */
public interface DataProducer extends Closeable {

    void setIn(DataIn dataIn);

    void setOut(DataOut dataOut);

    void produce();

}
