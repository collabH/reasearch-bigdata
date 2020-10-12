package org.research.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;

/**
 * @fileName: CuratorClient.java
 * @description: CuratorClient.java类说明
 * @author: by echo huang
 * @date: 2020/10/12 11:35 上午
 */
public class CuratorClient {
    public static void main(String[] args) throws Exception {
//        createClient();
//        createNode();
//        deleteNode();
        readNode();
//        updateNode();
    }

    /**
     * 创建会话
     */
    public static CuratorFramework createClient() {
        // 创建curator客户端
        RetryNTimes retryPolicy = new RetryNTimes(3, 5000);
        CuratorFramework client = CuratorFrameworkFactory.newClient("hadoop:2181", retryPolicy);

        // 第二种方式curator client
        CuratorFramework client2 = CuratorFrameworkFactory.builder()
                .connectString("hadoop:2181")
                .sessionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .namespace("curator")
                .build();
        // 启动
//        client.start();

//        client.close();
//        return client;
        client2.start();

        return client2;
    }

    /**
     * 创建节点
     */
    public static void createNode() throws Exception {
        CuratorFramework client = createClient();
        String path = client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/create-node/hello", "hello".getBytes());
        System.out.println(path);
    }

    /**
     * 删除节点
     *
     * @throws Exception
     */
    public static void deleteNode() throws Exception {
        CuratorFramework client = createClient();

        client.delete()
                .guaranteed() //保证强制删除node
                .deletingChildrenIfNeeded() //递归删除全部路径
                .withVersion(0)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println(event.toString());
                    }
                })
                .forPath("/create-node/hello");
    }

    /**
     * 读取数据
     *
     * @throws Exception
     */
    public static void readNode() throws Exception {
        CuratorFramework client = createClient();
        Stat stat = new Stat();
        byte[] bytes = client.getData()
                // 读取该节点的stat
                .storingStatIn(stat)
                .forPath("/create-node/hello");

        System.out.println(new String(bytes, Charset.defaultCharset()));
        System.out.println(stat.toString());
    }

    /**
     * 修改数据
     *
     * @throws Exception
     */
    public static void updateNode() throws Exception {
        CuratorFramework client = createClient();

        client.setData()
                .withVersion(0)
                .forPath("/create-node/hello", "zhangsan".getBytes());
    }

    /**
     * 异步操作
     *
     * @throws Exception
     */
    public static void asyncOpeator() throws Exception {
        CuratorFramework client = createClient();

        client.getData()
                .inBackground(new BackgroundCallback() {
                    /**
                     *
                     * @param client 当前客户端实例
                     * @param event 服务端事件
                     * @throws Exception
                     */
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println(event.getType());
                    }
                });
    }

}
