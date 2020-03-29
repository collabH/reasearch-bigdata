package com.research.hadoop.libraryclass;

/**
 * @fileName: ChainMR.java
 * @description: ChainMR.java类说明
 * @author: by echo huang
 * @date: 2020-03-28 13:43
 */
public class ChainMR {
    public static void main(String[] args) {
//        ChainMapper.addMapper();
        for (String arg : args) {
            System.out.println(arg);
        }
        System.out.println(bTOGB(12829060788L));
    }

    public static long bTOGB(long size) {
        return size >> 30;
    }
}
