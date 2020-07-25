package com.research.hadoop.join.reduce;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @fileName: TableBean.java
 * @description: TableBean.java类说明
 * @author: by echo huang
 * @date: 2020-07-25 11:48
 */
@Data
public class TableBean implements WritableComparable<TableBean> {

    private String id;
    private String pid;
    private int amount;
    private String pName;
    private String flag;

    @Override
    public int compareTo(TableBean o) {
        return Integer.valueOf(id).compareTo(Integer.valueOf(o.getId()));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pName);
        dataOutput.writeUTF(flag);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        pid = dataInput.readUTF();
        amount = dataInput.readInt();
        pName = dataInput.readUTF();
        flag = dataInput.readUTF();
    }
}
