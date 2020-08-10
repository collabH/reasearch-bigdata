package org.telecome.common.domain;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * @fileName: DataIn.java
 * @description: DataIn.java类说明
 * @author: by echo huang
 * @date: 2020-08-10 22:35
 */
public interface DataIn extends Closeable {
    void setPath(String path);

    Object read() throws IOException;

    <T extends Data> List<T> read(Class<T> clazz) throws IOException;
}
