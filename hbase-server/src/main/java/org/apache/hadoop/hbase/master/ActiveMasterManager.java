/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles everything on master-side related to master election.
 * 处理与主选举有关的主方面的一切。
 *
 * <p>Listens and responds to ZooKeeper notifications on the master znode,
 * both <code>nodeCreated</code> and <code>nodeDeleted</code>.
 * 侦听并响应主znode上的ZooKeeper通知：nodeCreated 和 nodeDeleted
 *
 * <p>Contains blocking methods which will hold up backup masters, waiting
 * for the active master to fail.
 * 包含阻止方法，这些方法将阻止备用主服务器，并等待*等待活动主服务器发生故障。
 *
 * <p>This class is instantiated in the HMaster constructor and the method
 * #blockUntilBecomingActiveMaster() is called to wait until becoming
 * the active master of the cluster.
 * 此类在HMaster构造函数中实例化，并调用方法#blockUntilBecomingActiveMaster() 等待，直到成为集群的活动主控方为止。
 */
@InterfaceAudience.Private
public class ActiveMasterManager extends ZKListener {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMasterManager.class);

    final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
    final AtomicBoolean clusterShutDown = new AtomicBoolean(false);

    private final ServerName sn;
    private int infoPort;
    private final Server master;

    /**
     * @param watcher
     * @param sn      ServerName
     * @param master  In an instance of a Master.
     */
    ActiveMasterManager(ZKWatcher watcher, ServerName sn, Server master) {
        super(watcher);
        watcher.registerListener(this);
        this.sn = sn;
        this.master = master;
    }

    // will be set after jetty server is started
    public void setInfoPort(int infoPort) {
        this.infoPort = infoPort;
    }

    @Override
    public void nodeCreated(String path) {
        handle(path);
    }

    @Override
    public void nodeDeleted(String path) {

        // We need to keep track of the cluster's shutdown status while
        // we wait on the current master. We consider that, if the cluster
        // was already in a "shutdown" state when we started, that this master
        // is part of a new cluster that was started shortly after the old cluster
        // shut down, so that state is now irrelevant. This means that the shutdown
        // state must be set while we wait on the active master in order
        // to shutdown this master. See HBASE-8519.
        if(path.equals(watcher.getZNodePaths().clusterStateZNode) && !master.isStopped()) {
            clusterShutDown.set(true);
        }

        handle(path);
    }

    void handle(final String path) {
        if(path.equals(watcher.getZNodePaths().masterAddressZNode) && !master.isStopped()) {
            handleMasterNodeChange();
        }
    }

    /**
     * Handle a change in the master node.  Doesn't matter whether this was called
     * from a nodeCreated or nodeDeleted event because there are no guarantees
     * that the current state of the master node matches the event at the time of
     * our next ZK request.
     *
     * <p>Uses the watchAndCheckExists method which watches the master address node
     * regardless of whether it exists or not.  If it does exist (there is an
     * active master), it returns true.  Otherwise it returns false.
     *
     * <p>A watcher is set which guarantees that this method will get called again if
     * there is another change in the master node.
     */
    private void handleMasterNodeChange() {
        // Watch the node and check if it exists.
        try {
            synchronized(clusterHasActiveMaster) {
                if(ZKUtil.watchAndCheckExists(watcher, watcher.getZNodePaths().masterAddressZNode)) {
                    // A master node exists, there is an active master
                    LOG.trace("A master is now available");
                    clusterHasActiveMaster.set(true);
                } else {
                    // Node is no longer there, cluster does not have an active master
                    LOG.debug("No master available. Notifying waiting threads");
                    clusterHasActiveMaster.set(false);
                    // Notify any thread waiting to become the active master
                    clusterHasActiveMaster.notifyAll();
                }
            }
        } catch(KeeperException ke) {
            master.abort("Received an unexpected KeeperException, aborting", ke);
        }
    }

    /**
     * Block until becoming the active master.
     *
     * Method blocks until there is not another active master and our attempt
     * to become the new active master is successful.
     *
     * This also makes sure that we are watching the master znode so will be
     * notified if another master dies.
     *
     * @param checkInterval the interval to check if the master is stopped
     * @param startupStatus the monitor status to track the progress
     * @return True if no issue becoming active master else false if another
     * master was running or if some other problem (zookeeper, stop flag has been
     * set on this Master)
     */
    boolean blockUntilBecomingActiveMaster(int checkInterval, MonitoredTask startupStatus) {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 注册和争抢成为 Active Master
         */
        String backupZNode = ZNodePaths.joinZNode(this.watcher.getZNodePaths().backupMasterAddressesZNode, this.sn.toString());

        while(!(master.isAborted() || master.isStopped())) {
            startupStatus.setStatus("Trying to register in ZK as active master");

            // Try to become the active master, watch if there is another master.
            // Write out our ServerName as versioned bytes.
            try {

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 去注册 Active Master, 记住：也是临时节点
                 *   分布式锁的方式来实现的！
                 *   MasterAddressTracker.setMasterAddress() 创建一个 临时 znode 节点，
                 *   多个 hmsater 如果同时启动，则可能同时都在创建这个 代表 active 身份的 znode 节点
                 *   谁创建成功了，谁才能成为 active
                 *  所以：如下的代码，如果能进入到 if ，就证明当前 hmaster 争抢这把分布式锁成功了。
                 *  当前 hmaster 才能成为 active
                 *
                 *  接着再做一件事：就是删除之前一上线 就创建的 backup znode 节点
                 */
                if(MasterAddressTracker.setMasterAddress(this.watcher, this.watcher.getZNodePaths().masterAddressZNode, this.sn, infoPort)) {

                    /********
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *   注释： 如果注册成功，则删除自己原先创建的 backup master znode 临时节点
                     */
                    // If we were a backup master before, delete our ZNode from the backup master directory since we are the active now)
                    if(ZKUtil.checkExists(this.watcher, backupZNode) != -1) {
                        LOG.info("Deleting ZNode for " + backupZNode + " from backup master directory");
                        ZKUtil.deleteNodeFailSilent(this.watcher, backupZNode);
                    }

                    /********
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *   注释： 将znode保存在文件中，这将允许检查启动脚本是否崩溃
                     */
                    // Save the znode in a file, this will allow to check if we crash in the launch scripts
                    ZNodeClearer.writeMyEphemeralNodeOnDisk(this.sn.toString());


                    /********
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *   注释： 设置 Master 为 Active 状态
                     */
                    // We are the master, return
                    startupStatus.setStatus("Successfully registered as active master.");
                    this.clusterHasActiveMaster.set(true);
                    LOG.info("Registered as active master=" + this.sn);
                    return true;
                }
                // TODO_MA 注释： 从这儿开始，active　master 就不糊在执行了。
                // TODO_MA 注释： 如果是 backup master 就继续往下执行
                // TODO_MA 注释： backup master 必须要去 zookeeper 中监控 active amster 的znode节点
                // TODO_MA 注释：如果发现 active mastre 的znode节点如果不在了，则证明active master死掉了。则当前hamster 去竞争active 角色
                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释：
                 *   1、要获取到底谁是 active
                 *   2、要去监控这个active master 的znode节点
                 */

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 设置集群有 Active Master 了
                 */
                // There is another active master running elsewhere or this is a restart
                // and the master ephemeral node has not expired yet.
                this.clusterHasActiveMaster.set(true);

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 到上面为止，也只是知道有 Active Master了，但是到底是谁不知道
                 *   所以通过获取 Active Master ZNode节点的数据得知
                 */
                String msg;
                byte[] bytes = ZKUtil.getDataAndWatch(this.watcher, this.watcher.getZNodePaths().masterAddressZNode);
                if(bytes == null) {
                    msg = ("A master was detected, but went down before its address " + "could be read.  Attempting to become the next active master");
                } else {

                    /********
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *   注释： 这个分支是正常情况
                     */
                    ServerName currentMaster;
                    try {

                        /********
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *   注释： 获取当前 Active Master是谁
                         */
                        currentMaster = ProtobufUtil.parseServerNameFrom(bytes);

                    } catch(DeserializationException e) {
                        LOG.warn("Failed parse", e);
                        // Hopefully next time around we won't fail the parse.  Dangerous.
                        continue;
                    }

                    /********
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *   注释： 如果代码能走到这儿，说明当前master不是active，则自己在master节点下的znode要删除
                     */
                    if(ServerName.isSameAddress(currentMaster, this.sn)) {
                        msg = ("Current master has this master's address, " + currentMaster + "; master was restarted? Deleting node.");
                        // Hurry along the expiration of the znode.
                        ZKUtil.deleteNode(this.watcher, this.watcher.getZNodePaths().masterAddressZNode);

                        // We may have failed to delete the znode at the previous step, but
                        //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
                        ZNodeClearer.deleteMyEphemeralNodeOnDisk();
                    } else {
                        msg = "Another master is the active master, " + currentMaster + "; waiting to become the next active master";
                    }
                }
                LOG.info(msg);
                startupStatus.setStatus(msg);
            } catch(KeeperException ke) {
                master.abort("Received an unexpected KeeperException, aborting", ke);
                return false;
            }

            synchronized(this.clusterHasActiveMaster) {
                while(clusterHasActiveMaster.get() && !master.isStopped()) {
                    try {
                        clusterHasActiveMaster.wait(checkInterval);
                    } catch(InterruptedException e) {
                        // We expect to be interrupted when a master dies, will fall out if so
                        LOG.debug("Interrupted waiting for master to die", e);
                    }
                }
                if(clusterShutDown.get()) {
                    this.master.stop("Cluster went down before this master became active");
                }
            }
        }
        return false;
    }

    /**
     * @return True if cluster has an active master.
     */
    boolean hasActiveMaster() {
        try {

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 校验是否有active主节点的方法就是去 zookeeper 找active master 父节点下的子节点个数是否大于0
             */
            if(ZKUtil.checkExists(watcher, watcher.getZNodePaths().masterAddressZNode) >= 0) {
                return true;
            }
        } catch(KeeperException ke) {
            LOG.info("Received an unexpected KeeperException when checking " + "isActiveMaster : " + ke);
        }
        return false;
    }

    public void stop() {
        try {
            synchronized(clusterHasActiveMaster) {
                // Master is already stopped, wake up the manager
                // thread so that it can shutdown soon.
                clusterHasActiveMaster.notifyAll();
            }
            // If our address is in ZK, delete it on our way out
            ServerName activeMaster = null;
            try {
                activeMaster = MasterAddressTracker.getMasterAddress(this.watcher);
            } catch(IOException e) {
                LOG.warn("Failed get of master address: " + e.toString());
            }
            if(activeMaster != null && activeMaster.equals(this.sn)) {
                ZKUtil.deleteNode(watcher, watcher.getZNodePaths().masterAddressZNode);
                // We may have failed to delete the znode at the previous step, but
                //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
                ZNodeClearer.deleteMyEphemeralNodeOnDisk();
            }
        } catch(KeeperException e) {
            LOG.debug(this.watcher.prefix("Failed delete of our master address node; " + e.getMessage()));
        }
    }
}
