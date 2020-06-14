package com.research.hadoop.common.ipc;

/**
 * @fileName: IPCQueryServer.java
 * @description: IPCQueryServer.java类说明
 * @author: by echo huang
 * @date: 2020-06-11 19:16
 */
public class IPCQueryServer {
    public static final int IPC_PORT = 3212;
    public static final long IPC_VER = 5473L;

    public static void main(String[] args) {
        IPCQueryStatus queryStatus = new IPCQueryStatusImpl();
    }
}
