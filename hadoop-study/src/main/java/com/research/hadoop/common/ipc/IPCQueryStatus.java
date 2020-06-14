package com.research.hadoop.common.ipc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @fileName: IPCQueryStatus.java
 * @description: hadoop ipc查询状态接口
 * @author: by echo huang
 * @date: 2020-06-11 19:01
 */
public interface IPCQueryStatus extends VersionedProtocol {
    /**
     * 查询文件状态
     *
     * @param fileName
     * @return
     */
    String getFileStauts(String fileName);
}
