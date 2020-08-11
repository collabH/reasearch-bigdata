package org.telecome.producer;

import org.telecome.common.domain.DataProducer;
import org.telecome.producer.domain.LocalFileProducer;
import org.telecome.producer.io.LocalFileDataIn;
import org.telecome.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * @fileName: Bootstrap.java
 * @description: 启动对象
 * @author: by echo huang
 * @date: 2020-08-10 22:41
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        DataProducer producer = new LocalFileProducer();
        LocalFileDataIn dataIn = new LocalFileDataIn();
        //
        dataIn.setPath("/Users/babywang/Downloads/contact.log");
        producer.setIn(dataIn);

        LocalFileDataOut dataOut = new LocalFileDataOut();
        dataOut.setPath("/Users/babywang/Downloads/call.log");
        producer.setOut(dataOut);

        producer.produce();
        producer.close();
    }
}
