package org.telecome.producer.io;

import org.telecome.common.domain.DataOut;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Objects;

/**
 * @fileName: LocalFileDataOut.java
 * @description: LocalFileDataOut.java类说明
 * @author: by echo huang
 * @date: 2020-08-11 21:25
 */
public class LocalFileDataOut implements DataOut {

    private PrintWriter write;

    @Override
    public void setPath(String path) {

        try {
            write = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), Charset.defaultCharset()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Object data) throws IOException {
        write(data.toString());
    }

    @Override
    public void write(String data) throws IOException {
        write.println(data);
        write.flush();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(write)) {
            write.close();
        }
    }
}
