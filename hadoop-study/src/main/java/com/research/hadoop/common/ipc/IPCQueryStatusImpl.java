package com.research.hadoop.common.ipc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * @fileName: IPCQueryStatusImpl.java
 * @description: IPCQueryStatusImpl.java类说明
 * @author: by echo huang
 * @date: 2020-06-11 19:14
 */
public class IPCQueryStatusImpl implements IPCQueryStatus {
    @Override
    public String getFileStauts(String fileName) {

        return null;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return null;
    }
}
