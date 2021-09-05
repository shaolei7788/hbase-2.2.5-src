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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.ServerCommandLine;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.GnuParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;

@InterfaceAudience.Private
public class HMasterCommandLine extends ServerCommandLine {
    private static final Logger LOG = LoggerFactory.getLogger(HMasterCommandLine.class);

    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释： 当如果启动命令有误，则会输出这段提示
     */
    private static final String USAGE = "Usage: Master [opts] start|stop|clear\n" + " start  Start Master. If local mode, start Master and RegionServer in same JVM\n" + " stop   Start cluster shutdown; Master signals RegionServer shutdown\n" + " clear  Delete the master znode in ZooKeeper after a master crashes\n " + " where [opts] are:\n" + "   --minRegionServers=<servers>   Minimum RegionServers needed to host user tables.\n" + "   --localRegionServers=<servers> " + "RegionServers to start in master process when in standalone mode.\n" + "   --masters=<servers>            Masters to start in this process.\n" + "   --backup                       Master should start in backup mode";


    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释： 构造方法传进来的成员变量，也就是启动的 具体的 Master 的实例
     */
    private final Class<? extends HMaster> masterClass;
    public HMasterCommandLine(Class<? extends HMaster> masterClass) {
        this.masterClass = masterClass;
    }

    @Override
    protected String getUsage() {
        return USAGE;
    }

    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释：
     */
    @Override
    public int run(String args[]) throws Exception {

        // TODO_MA 注释：可选参数的配置问题，初始化
        Options opt = new Options();
        opt.addOption("localRegionServers", true, "RegionServers to start in master process when running standalone");
        opt.addOption("masters", true, "Masters to start in this process");
        opt.addOption("minRegionServers", true, "Minimum RegionServers needed to host user tables");
        opt.addOption("backup", false, "Do not try to become HMaster until the primary fails");

        CommandLine cmd;
        try {

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 参数解析
             */
            cmd = new GnuParser().parse(opt, args);

        } catch(ParseException e) {
            LOG.error("Could not parse: ", e);
            usage(null);
            return 1;
        }

        if(cmd.hasOption("minRegionServers")) {
            String val = cmd.getOptionValue("minRegionServers");
            getConf().setInt("hbase.regions.server.count.min", Integer.parseInt(val));
            LOG.debug("minRegionServers set to " + val);
        }

        // minRegionServers used to be minServers.  Support it too.
        if(cmd.hasOption("minServers")) {
            String val = cmd.getOptionValue("minServers");
            getConf().setInt("hbase.regions.server.count.min", Integer.parseInt(val));
            LOG.debug("minServers set to " + val);
        }

        // check if we are the backup master - override the conf if so
        if(cmd.hasOption("backup")) {
            getConf().setBoolean(HConstants.MASTER_TYPE_BACKUP, true);
        }

        // How many regionservers to startup in this process (we run regionservers in same process as
        // master when we are in local/standalone mode. Useful testing)
        if(cmd.hasOption("localRegionServers")) {
            String val = cmd.getOptionValue("localRegionServers");
            getConf().setInt("hbase.regionservers", Integer.parseInt(val));
            LOG.debug("localRegionServers set to " + val);
        }

        // How many masters to startup inside this process; useful testing
        if(cmd.hasOption("masters")) {
            String val = cmd.getOptionValue("masters");
            getConf().setInt("hbase.masters", Integer.parseInt(val));
            LOG.debug("masters set to " + val);
        }

        // TODO_MA 注释： cmd 变成 remainingArgs
        // TODO_MA 注释：cmd = CommandLine
        @SuppressWarnings("unchecked")
        List<String> remainingArgs = cmd.getArgList();

        // start  stop
        if(remainingArgs.size() != 1) {
            usage(null);
            return 1;
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 提取第一个参数，一般来说，都是使用命令比如：  start-hbase.sh 启动
         *   底层启动的命令是： hbase-daemon.sh start master 和 hbase-daemon.sh start regionserver
         *   然后底层真正执行的是：
         *   1、java org.apache.hadoop.hbase.master.HMaster start 启动 master
         *   2、java org.apache.hadoop.hbase.regionserver.HRegionServer start 启动 regionserver
         */
        String command = remainingArgs.get(0);

        if("start".equals(command)) {

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 启动 Master
             */
            return startMaster();

        } else if("stop".equals(command)) {

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 关闭 Master
             */
            return stopMaster();

        } else if("clear".equals(command)) {
            return (ZNodeClearer.clear(getConf()) ? 0 : 1);
        } else {
            usage("Invalid command: " + command);
            return 1;
        }
    }

    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释： 启动 HMaster
     */
    private int startMaster() {
        Configuration conf = getConf();
        TraceUtil.initTracer(conf);

        try {

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 本地模式
             *   启动 master 和 regionserver 在同一个节点，在同一个JVM中
             */
            // If 'local', defer to LocalHBaseCluster instance.
            // Starts master and regionserver both in the one JVM.
            if(LocalHBaseCluster.isLocal(conf)) {
                DefaultMetricsSystem.setMiniClusterMode(true);
                final MiniZooKeeperCluster zooKeeperCluster = new MiniZooKeeperCluster(conf);
                File zkDataPath = new File(conf.get(HConstants.ZOOKEEPER_DATA_DIR));

                // find out the default client port
                int zkClientPort = 0;

                // If the zookeeper client port is specified in server quorum, use it.
                String zkserver = conf.get(HConstants.ZOOKEEPER_QUORUM);
                if(zkserver != null) {
                    String[] zkservers = zkserver.split(",");

                    if(zkservers.length > 1) {
                        // In local mode deployment, we have the master + a region server and zookeeper server
                        // started in the same process. Therefore, we only support one zookeeper server.
                        String errorMsg = "Could not start ZK with " + zkservers.length + " ZK servers in local mode deployment. Aborting as clients (e.g. shell) will not " + "be able to find this ZK quorum.";
                        System.err.println(errorMsg);
                        throw new IOException(errorMsg);
                    }

                    String[] parts = zkservers[0].split(":");

                    if(parts.length == 2) {
                        // the second part is the client port
                        zkClientPort = Integer.parseInt(parts[1]);
                    }
                }
                // If the client port could not be find in server quorum conf, try another conf
                if(zkClientPort == 0) {
                    zkClientPort = conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 0);
                    // The client port has to be set by now; if not, throw exception.
                    if(zkClientPort == 0) {
                        throw new IOException("No config value for " + HConstants.ZOOKEEPER_CLIENT_PORT);
                    }
                }
                zooKeeperCluster.setDefaultClientPort(zkClientPort);
                // set the ZK tick time if specified
                int zkTickTime = conf.getInt(HConstants.ZOOKEEPER_TICK_TIME, 0);
                if(zkTickTime > 0) {
                    zooKeeperCluster.setTickTime(zkTickTime);
                }

                // login the zookeeper server principal (if using security)
                ZKUtil.loginServer(conf, HConstants.ZK_SERVER_KEYTAB_FILE, HConstants.ZK_SERVER_KERBEROS_PRINCIPAL, null);
                int localZKClusterSessionTimeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT + ".localHBaseCluster", 10 * 1000);
                conf.setInt(HConstants.ZK_SESSION_TIMEOUT, localZKClusterSessionTimeout);
                LOG.info("Starting a zookeeper cluster");
                int clientPort = zooKeeperCluster.startup(zkDataPath);
                if(clientPort != zkClientPort) {
                    String errorMsg = "Could not start ZK at requested port of " + zkClientPort + ".  ZK was started at port: " + clientPort + ".  Aborting as clients (e.g. shell) will not be able to find " + "this ZK quorum.";
                    System.err.println(errorMsg);
                    throw new IOException(errorMsg);
                }
                conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));

                // Need to have the zk cluster shutdown when master is shutdown.
                // Run a subclass that does the zk cluster shutdown on its way out.
                int mastersCount = conf.getInt("hbase.masters", 1);
                int regionServersCount = conf.getInt("hbase.regionservers", 1);
                // Set start timeout to 5 minutes for cmd line start operations
                conf.setIfUnset("hbase.master.start.timeout.localHBaseCluster", "300000");
                LOG.info("Starting up instance of localHBaseCluster; master=" + mastersCount + ", regionserversCount=" + regionServersCount);
                LocalHBaseCluster cluster = new LocalHBaseCluster(conf, mastersCount, regionServersCount, LocalHMaster.class, HRegionServer.class);
                ((LocalHMaster) cluster.getMaster(0)).setZKCluster(zooKeeperCluster);
                cluster.startup();
                waitOnMasterThreads(cluster);

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 集群模式
                 */
            } else {
                logProcessInfo(getConf());

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 通过反射构造 HMaster 实例， HMaster 是 HRegionServer 的子类，
                 *   1、先看 HMaster 中的构造方法做了什么
                 *   2、再进入 Hmaster 的 run() 方法看看做了什么
                 *   3、masterClass = HMaster.class
                 */
                HMaster master = HMaster.constructMaster(masterClass, conf);

                // TODO_MA 注释： 当我们还没有启动好 HMaster 的时候，结果就有请求发送过来说要关闭 HMaster
                if(master.isStopped()) {
                    LOG.info("Won't bring the Master up as a shutdown is requested");
                    return 1;
                }

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释：
                 *   1、hmaster 通过 start() 启动
                 *   2、hmaster 肯定也是一个线程
                 *   3、当执行 start 方法之后，应该转到 hmaster 的 run() 继续阅读
                 */
                master.start();
                master.join();

                if(master.isAborted())
                    throw new RuntimeException("HMaster Aborted");
            }
        } catch(Throwable t) {
            LOG.error("Master exiting", t);
            return 1;
        }
        return 0;
    }

    @SuppressWarnings("resource")
    private int stopMaster() {
        Configuration conf = getConf();
        // Don't try more than once
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
        try(Connection connection = ConnectionFactory.createConnection(conf)) {
            try(Admin admin = connection.getAdmin()) {
                admin.shutdown();
            } catch(Throwable t) {
                LOG.error("Failed to stop master", t);
                return 1;
            }
        } catch(MasterNotRunningException e) {
            LOG.error("Master not running");
            return 1;
        } catch(ZooKeeperConnectionException e) {
            LOG.error("ZooKeeper not available");
            return 1;
        } catch(IOException e) {
            LOG.error("Got IOException: " + e.getMessage(), e);
            return 1;
        }
        return 0;
    }

    private void waitOnMasterThreads(LocalHBaseCluster cluster) throws InterruptedException {
        List<JVMClusterUtil.MasterThread> masters = cluster.getMasters();
        List<JVMClusterUtil.RegionServerThread> regionservers = cluster.getRegionServers();

        if(masters != null) {
            for(JVMClusterUtil.MasterThread t : masters) {
                t.join();
                if(t.getMaster().isAborted()) {
                    closeAllRegionServerThreads(regionservers);
                    throw new RuntimeException("HMaster Aborted");
                }
            }
        }
    }

    private static void closeAllRegionServerThreads(List<JVMClusterUtil.RegionServerThread> regionservers) {
        for(JVMClusterUtil.RegionServerThread t : regionservers) {
            t.getRegionServer().stop("HMaster Aborted; Bringing down regions servers");
        }
    }

    /*
     * Version of master that will shutdown the passed zk cluster on its way out.
     */
    public static class LocalHMaster extends HMaster {
        private MiniZooKeeperCluster zkcluster = null;

        public LocalHMaster(Configuration conf) throws IOException, KeeperException, InterruptedException {
            super(conf);
        }

        @Override
        public void run() {
            super.run();
            if(this.zkcluster != null) {
                try {
                    this.zkcluster.shutdown();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }

        void setZKCluster(final MiniZooKeeperCluster zkcluster) {
            this.zkcluster = zkcluster;
        }
    }
}
