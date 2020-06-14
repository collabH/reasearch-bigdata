package com.research.hadoop.common.socket;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @fileName: SocketTest.java
 * @description: SocketTest.java类说明
 * @author: by echo huang
 * @date: 2020-06-09 19:44
 */
public class SocketTest {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("localhost", 9999);
        InputStream inputStream = socket.getInputStream();
        byte[] data = new byte[1024];
        inputStream.read(data);
        System.out.println(new String(data, Charsets.UTF_8));
        serverSocket();
    }

    /**
     * udp连接
     *
     * @throws IOException
     */
    private static void serverSocket() throws IOException {
        ServerSocket serverSocket = new ServerSocket();
        byte[] data = new byte[1024];
        serverSocket.accept().getInputStream().read(data);
        System.out.println(new String(data, Charsets.UTF_8));
    }
}
