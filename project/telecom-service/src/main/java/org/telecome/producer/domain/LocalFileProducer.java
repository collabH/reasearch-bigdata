package org.telecome.producer.domain;

import org.telecome.common.domain.DataIn;
import org.telecome.common.domain.DataOut;
import org.telecome.common.domain.DataProducer;
import org.telecome.common.utils.NumberUtil;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;

/**
 * @fileName: LocalFileProducer.java
 * @description: 本地数据文件生产者
 * @author: by echo huang
 * @date: 2020-08-10 22:42
 */
public class LocalFileProducer implements DataProducer {

    private DataIn in;
    private DataOut out;

    private volatile boolean flag = true;

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
            while (flag) {
                int call1Index = new Random().nextInt(read.size());
                int call2Index;
                while (true) {
                    call2Index = new Random().nextInt(read.size());
                    if (call1Index != call2Index) {
                        break;
                    }
                }
                Contact call1 = read.get(call1Index);
                Contact call2 = read.get(call2Index);

                String start = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

                // 通话时长
                String duration = NumberUtil.fromat(new Random().nextInt(1800), 4);
                Calllog calllog = Calllog.builder()
                        .call1(call1.getPhone())
                        .call2(call2.getPhone())
                        .calltime(start)
                        .duration(duration).build();

//                System.out.println(calllog);
                out.write(calllog);

                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws IOException {

    }

    public static void main(String[] args) {

    }
}
