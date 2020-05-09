package com.research.hadoop.join.reduce;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @fileName: TextPair.java
 * @description: TextPair.java类说明
 * @author: by echo huang
 * @date: 2020-03-27 11:29
 */
public class TextPair implements WritableComparable<TextPair> {
    private Integer id;
    private String sign;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getSign() {
        return sign;
    }

    public void setSign(String sign) {
        this.sign = sign;
    }

    public TextPair() {
    }

    public TextPair(Integer id, String sign) {
        this.id = id;
        this.sign = sign;
    }

    @Override
    public int compareTo(TextPair o) {
        return this.id.compareTo(o.getId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.sign);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.sign = in.readUTF();
    }

    static class FirstCompartor implements RawComparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compare(s1, s2);
        }
    }
}
