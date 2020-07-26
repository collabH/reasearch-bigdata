package org.research.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;

@Slf4j
public class ZkNodeOperator implements Watcher {
    public ZooKeeper zk = null;
    private static final Integer timeout = 5000;

    public ZkNodeOperator() {

    }

    public ZkNodeOperator(String connectString) {
        try {
            zk = new ZooKeeper(connectString, timeout, this);
        } catch (IOException e) {
            log.warn("e", e);
            if (zk != null) {
                try {
                    zk.close();
                } catch (InterruptedException ex) {
                    log.warn("e", e);
                }
            }
        }
    }


    public String createNode(String path, byte[] data, List<ACL> aclList) {
        String result = "";
        /**
         * 同步或者异步创建节点，都不支持子节点的递归创建，异步有一个callback函数
         * 参数：
         * path：创建的路径
         * data：存储的数据的byte[]
         * acl：控制权限策略
         * 			Ids.OPEN_ACL_UNSAFE --> world:anyone:cdrwa
         * 			CREATOR_ALL_ACL --> auth:user:password:cdrwa
         * createMode：节点类型, 是一个枚举
         * 			PERSISTENT：持久节点
         * 			PERSISTENT_SEQUENTIAL：持久顺序节点
         * 			EPHEMERAL：临时节点
         * 			EPHEMERAL_SEQUENTIAL：临时顺序节点
         */
        //同步创建临时节点
//            result = zk.create(path, data, aclList, CreateMode.EPHEMERAL);
        try {
            String ctx = "{'create':'success'}";
            // 异步创建
            zk.create(path, data, aclList, CreateMode.PERSISTENT, (rc, p, c, name) -> {
                log.warn("创建节点:{}", path);
                log.warn("rc:{},ctx:{},name:{}", rc, c, name);
            }, ctx);
            log.warn("create result:{}", result);
            //fixme 睡眠为了防止节点还没创建成功，zk客户端就断开连接
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;

    }

    /**
     * 修改节点数据
     *
     * @param path
     * @param data
     * @param version
     */
    public void setNode(String path, byte[] data, int version) {/*
        try {

            //同步修改
            Stat stat = zk.setData(path, data, version);

            log.warn("stat:{}", stat);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }*/
        //异步修改
        String ctx = "{'create':'success'}";
        zk.setData(path, data, version, (rc, p, c, name) -> {
            log.warn("创建节点:{}", p);
            log.warn("rc:{},ctx:{},name:{}", rc, c, name);
        }, ctx);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void deleteNode(String path, int version) {
        try {
            //同步删除
            zk.delete(path, version);
            //异步删除
            //zk.delete(path, version, (i, s, o) -> log.warn("创建节点:{},{}.{}", i,s,o), "zhangsan");
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.warn("event:{}", watchedEvent);
    }
}

