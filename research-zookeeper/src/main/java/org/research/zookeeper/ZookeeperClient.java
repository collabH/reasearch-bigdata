package org.research.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @fileName: ZookeeperClient.java
 * @description: zk客户端操作
 * @author: by echo huang
 * @date: 2020-07-26 22:34
 */
@Slf4j
public class ZookeeperClient implements Record {

    private ZkNodeOperator zkNodeOperator;

    @Before
    public void initZkClient() throws IOException {
        zkNodeOperator = new ZkNodeOperator("localhost:2181");
    }

    @Test
    public void createZnode() throws KeeperException, InterruptedException {
        String s = zkNodeOperator.createNode("/name/wy", new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        System.out.println(s);
    }

    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        ZooKeeper zk = zkNodeOperator.zk;
        List<String> children = zk.getChildren("/name", System.out::println);

        children.forEach(System.out::println);
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void znodeIsExsits() throws KeeperException, InterruptedException {
        Stat exists = zkNodeOperator.zk.exists("/name/wy", false);
        System.out.println(exists);
    }

    @Override
    public void serialize(OutputArchive outputArchive, String s) throws IOException {
    }

    @Override
    public void deserialize(InputArchive inputArchive, String s) throws IOException {

    }
}
