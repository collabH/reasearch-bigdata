package org.telecome.producer.io;

import org.telecome.common.domain.Data;
import org.telecome.common.domain.DataIn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @fileName: LocalFileDataIn.java
 * @description: LocalFileDataIn.java类说明
 * @author: by echo huang
 * @date: 2020-08-10 23:34
 */
public class LocalFileDataIn implements DataIn {
    private BufferedReader reader;

    @Override
    public void setPath(String path) {
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path)), Charset.defaultCharset()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object read() throws IOException {
        return null;
    }

    @Override
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {
        List<T> contactList = new ArrayList<>();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                T t = clazz.newInstance();
                t.setValue(line);
                contactList.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return contactList;
    }


    @Override
    public void close() throws IOException {
        if (Objects.nonNull(reader)) {
            reader.close();
        }
    }
}
