package com.research.hadoop.nio;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;

/**
 * @fileName: NioTest.java
 * @description: NioTest.java类说明
 * @author: by echo huang
 * @date: 2020-06-10 20:19
 */
public class NioTest {
    static class NIOServer {
        public static void main(String[] args) throws IOException {
            Selector selector = Selector.open();
            ServerSocketChannel listenChannel = ServerSocketChannel.open();
            listenChannel.configureBlocking(false);
            listenChannel.socket().bind(new InetSocketAddress(9999));
            listenChannel.register(selector, SelectionKey.OP_ACCEPT);
            //循环接收
            while (true) {
                //如果返回值大于0代表有IO事件等待处理
                if (selector.select(1000) == 0) {
                    continue;
                }
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey next = iterator.next();
                    iterator.remove();
                }
            }
        }

    }

    static class NIOServerConnection {

    }
}
