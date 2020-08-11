package org.telecome.common.domain;

import java.io.Closeable;
import java.io.IOException;

/**
 * @fileName: DataOut.java
 * @description: DataOut.java类说明
 * @author: by echo huang
 * @date: 2020-08-10 22:35
 */
public interface DataOut extends Closeable {
    void setPath(String path);


    void write(Object data) throws IOException;

    void write(String data) throws IOException;

}
