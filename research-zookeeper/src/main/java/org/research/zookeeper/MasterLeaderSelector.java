package org.research.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

/**
 * @fileName: MasterLeaderSelector.java
 * @description: MasterLeaderSelector.java类说明
 * @author: by echo huang
 * @date: 2020/10/12 5:03 下午
 */
public class MasterLeaderSelector {
    public static void main(String[] args) throws InterruptedException {
        CuratorFramework client = CuratorClient.createClient();

        LeaderSelector leaderSelector = new LeaderSelector(client, "/leaderselector/master", new LeaderSelectorListener() {
            // 成为leader
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                System.out.println("成为Master");
                Thread.sleep(3000);
                System.out.println("完成master操作 释放master权力");
            }

            // 状态变更
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
                    throw new CancelLeadershipException();
                }
            }
        });
        leaderSelector.autoRequeue();
        leaderSelector.start();
        Thread.sleep(Integer.MAX_VALUE);
    }
}
