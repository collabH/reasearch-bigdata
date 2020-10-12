package org.research.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;

/**
 * @fileName: CuratorWatcher.java
 * @description: CuratorWatcher.java类说明
 * @author: by echo huang
 * @date: 2020/10/12 4:35 下午
 */
public class CuratorWatcher {
    public static void main(String[] args) throws Exception {
//        addWatcher();
        createChildrenNode();
    }

    public static void addWatcher() throws Exception {
        CuratorFramework client = CuratorClient.createClient();

        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/watcher", "hello".getBytes());

        NodeCache nodeCache = new NodeCache(client, "/watcher", false);

        nodeCache.start();
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("我变化了");
            }
        });

        client.setData().forPath("/watcher", "test".getBytes());

        Thread.sleep(2000);
        client.delete().deletingChildrenIfNeeded().forPath("/watcher");

        Thread.sleep(Integer.MAX_VALUE);
    }

    public static void createChildrenNode() throws Exception {
        CuratorFramework client = CuratorClient.createClient();

        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/childwatcher/test", "hello".getBytes());

        PathChildrenCache childrenCache = new PathChildrenCache(client, "/childwatcher", true);
        childrenCache.start();

        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println(pathChildrenCacheEvent.getType());
            }
        });

        Thread.sleep(2000);

        // update
        client.setData()
                .withVersion(0)
                .forPath("/childwatcher/test", "hh".getBytes());

        // 添加新的子节点
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/childwatcher/test1", "hello".getBytes());


        // 删除子节点
        client.delete()
                .withVersion(0)
                .forPath("/childwatcher/test1");

        Thread.sleep(Integer.MAX_VALUE);
    }
}
