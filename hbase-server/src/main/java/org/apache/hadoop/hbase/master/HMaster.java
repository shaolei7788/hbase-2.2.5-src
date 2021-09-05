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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Service;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.favored.FavoredNodesPromoter;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.MasterRpcServices.BalanceSwitchMode;
import org.apache.hadoop.hbase.master.assignment.AssignProcedure;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.MoveRegionProcedure;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.master.assignment.UnassignProcedure;
import org.apache.hadoop.hbase.master.balancer.BalancerChore;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.ClusterStatusChore;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.cleaner.ReplicationBarrierCleaner;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerChore;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerFactory;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DisableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.EnableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.InitMetaProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil.NonceProcedureRunnable;
import org.apache.hadoop.hbase.master.procedure.ModifyTableProcedure;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.master.procedure.RecoverMetaProcedure;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.procedure.TruncateTableProcedure;
import org.apache.hadoop.hbase.master.replication.AddPeerProcedure;
import org.apache.hadoop.hbase.master.replication.DisablePeerProcedure;
import org.apache.hadoop.hbase.master.replication.EnablePeerProcedure;
import org.apache.hadoop.hbase.master.replication.ModifyPeerProcedure;
import org.apache.hadoop.hbase.master.replication.RemovePeerProcedure;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.master.replication.UpdatePeerConfigProcedure;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.master.zksyncer.MasterAddressSyncer;
import org.apache.hadoop.hbase.master.zksyncer.MetaLocationSyncer;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.monitoring.MemoryBoundedLogMessageBuffer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure.flush.MasterFlushTableProcedureManager;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureStoreListener;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.quotas.MasterQuotasObserver;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.SnapshotQuotaObserverChore;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotNotifier;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotNotifierFactory;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.master.ReplicationHFileCleaner;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.replication.master.ReplicationPeerConfigUpgrader;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationStatus;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.RegionNormalizerTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * HMaster 是 HBase 的 主服务， 一个 HBase 集群只能拥有一个 active master
 * HMaster is the "master server" for HBase. An HBase cluster has one active master.
 *
 * 如果启动了很多的 master，则他们将进行竞争上任。 竞争胜利的 master，管理整个 HBase 集群
 * If many masters are started, all compete.  Whichever wins goes on to run the cluster.
 *
 * All others park themselves in their constructor until master or cluster shutdown or until the active master loses its lease in
 * zookeeper.
 * 其他的 master（竞争失败的master）都阻塞在自己的 构造方法中，等待 active master 宕机 或者集群关闭了，
 * 或者 active master lose lease
 *
 * Thereafter, all running master jostle to take over master role.
 * 此后，所有正在运行的 master 夺 active master 。
 *
 * <p>The Master can be asked shutdown the cluster. See {@link #shutdown()}.
 * In this case it will tell all regionservers to go down and then wait on them all reporting in that they are down.
 * This master will then shut itself down.
 *
 * <p>You can also shutdown just this master.  Call {@link #stopMaster()}.
 * 所有其他主机都将自己停在其构造函数中，直到主机或群集关闭或活动主机失去其Zookeeper租约为止。 * *此后，所有正在运行的主角争夺主角角色。 * * <p>可以要求主服务器关闭群集。请参阅{@link #shutdown（）}。 *在这种情况下，它将告诉所有区域服务器停止运行，然后等待它们关闭的所有报告。 *然后该主机将自行关闭。 * * <p>您也可以仅关闭该主机。致电{@link #stopMaster（）}。
 *
 * @see org.apache.zookeeper.Watcher
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings("deprecation")
public class HMaster extends HRegionServer implements MasterServices {
    private static Logger LOG = LoggerFactory.getLogger(HMaster.class);

    /**
     * Protection against zombie master. Started once Master accepts active responsibility and
     * starts taking over responsibilities. Allows a finite time window before giving up ownership.
     */
    private static class InitializationMonitor extends HasThread {
        /**
         * The amount of time in milliseconds to sleep before checking initialization status.
         */
        public static final String TIMEOUT_KEY = "hbase.master.initializationmonitor.timeout";
        public static final long TIMEOUT_DEFAULT = TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);

        /**
         * When timeout expired and initialization has not complete, call {@link System#exit(int)} when
         * true, do nothing otherwise.
         */
        public static final String HALT_KEY = "hbase.master.initializationmonitor.haltontimeout";
        public static final boolean HALT_DEFAULT = false;

        private final HMaster master;
        private final long timeout;
        private final boolean haltOnTimeout;

        /**
         * Creates a Thread that monitors the {@link #isInitialized()} state.
         */
        InitializationMonitor(HMaster master) {
            super("MasterInitializationMonitor");
            this.master = master;
            this.timeout = master.getConfiguration().getLong(TIMEOUT_KEY, TIMEOUT_DEFAULT);
            this.haltOnTimeout = master.getConfiguration().getBoolean(HALT_KEY, HALT_DEFAULT);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while(!master.isStopped() && master.isActiveMaster()) {
                    Thread.sleep(timeout);
                    if(master.isInitialized()) {
                        LOG.debug("Initialization completed within allotted tolerance. Monitor exiting.");
                    } else {
                        LOG.error(
                                "Master failed to complete initialization after " + timeout + "ms. Please" + " consider submitting a bug report including a thread dump of this process.");
                        if(haltOnTimeout) {
                            LOG.error("Zombie Master exiting. Thread dump to stdout");
                            Threads.printThreadInfo(System.out, "Zombie HMaster");
                            System.exit(-1);
                        }
                    }
                }
            } catch(InterruptedException ie) {
                LOG.trace("InitMonitor thread interrupted. Existing.");
            }
        }
    }

    // MASTER is name of the webapp and the attribute name used stuffing this
    //instance into web context.
    public static final String MASTER = "master";

    // Manager and zk listener for master election
    private final ActiveMasterManager activeMasterManager;
    // Region server tracker
    private RegionServerTracker regionServerTracker;
    // Draining region server tracker
    private DrainingServerTracker drainingServerTracker;

    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释：加载balancer状态的跟踪器
     */
    // Tracker for load balancer state
    LoadBalancerTracker loadBalancerTracker;

    // Tracker for meta location, if any client ZK quorum specified
    MetaLocationSyncer metaLocationSyncer;
    // Tracker for active master location, if any client ZK quorum specified
    MasterAddressSyncer masterAddressSyncer;

    // Tracker for split and merge state
    private SplitOrMergeTracker splitOrMergeTracker;

    // Tracker for region normalizer state
    private RegionNormalizerTracker regionNormalizerTracker;

    private ClusterSchemaService clusterSchemaService;

    public static final String HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS = "hbase.master.wait.on.service.seconds";
    public static final int DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS = 5 * 60;

    // Metrics for the HMaster
    final MetricsMaster metricsMaster;

    // TODO_MA 注释：专门负责 master 和 HDFS 交互的类
    // file system manager for the master FS operations
    private MasterFileSystem fileSystemManager;
    private MasterWalManager walManager;

    // manager to manage procedure-based WAL splitting, can be null if current
    // is zk-based WAL splitting. SplitWALManager will replace SplitLogManager
    // and MasterWalManager, which means zk-based WAL splitting code will be
    // useless after we switch to the procedure-based one. our eventual goal
    // is to remove all the zk-based WAL splitting code.
    private SplitWALManager splitWALManager;

    // TODO_MA 注释：专门用于管理Region Server的管理器
    // server manager to deal with region server info
    private volatile ServerManager serverManager;

    // TODO_MA 注释：专门用于管理zk当中nodes的节点
    // manager of assignment nodes in zookeeper
    private AssignmentManager assignmentManager;

    // manager of replication
    private ReplicationPeerManager replicationPeerManager;

    // buffer for "fatal error" notices from region servers
    // in the cluster. This is only used for assisting
    // operations/debugging.
    MemoryBoundedLogMessageBuffer rsFatals;

    // flag set after we become the active master (used for testing)
    private volatile boolean activeMaster = false;

    // flag set after we complete initialization once active
    private final ProcedureEvent<?> initialized = new ProcedureEvent<>("master initialized");

    // flag set after master services are started,
    // initialization may have not completed yet.
    volatile boolean serviceStarted = false;

    // Maximum time we should run balancer for
    private final int maxBlancingTime;
    // Maximum percent of regions in transition when balancing
    private final double maxRitPercent;

    private final LockManager lockManager = new LockManager(this);

    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释：
     *   1、实现Region动态负载均衡的实体
     *   2、完成动态负载均衡的工作线程
     */
    private LoadBalancer balancer;
    private BalancerChore balancerChore;

    private RegionNormalizer normalizer;
    private RegionNormalizerChore normalizerChore;
    private ClusterStatusChore clusterStatusChore;
    private ClusterStatusPublisher clusterStatusPublisherChore = null;

    private HbckChore hbckChore;
    CatalogJanitor catalogJanitorChore;
    private DirScanPool cleanerPool;
    private LogCleaner logCleaner;
    private HFileCleaner hfileCleaner;
    private ReplicationBarrierCleaner replicationBarrierCleaner;
    private ExpiredMobFileCleanerChore expiredMobFileCleanerChore;
    private MobCompactionChore mobCompactChore;
    private MasterMobCompactionThread mobCompactThread;
    // used to synchronize the mobCompactionStates
    private final IdLock mobCompactionLock = new IdLock();
    // save the information of mob compactions in tables.
    // the key is table name, the value is the number of compactions in that table.
    private Map<TableName, AtomicInteger> mobCompactionStates = Maps.newConcurrentMap();

    MasterCoprocessorHost cpHost;

    private final boolean preLoadTableDescriptors;

    // Time stamps for when a hmaster became active
    private long masterActiveTime;

    // Time stamp for when HMaster finishes becoming Active Master
    private long masterFinishedInitializationTime;

    Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

    // TODO_MA 注释：负责监控表的备份
    // monitor for snapshot of hbase tables
    SnapshotManager snapshotManager;

    // monitor for distributed procedures
    private MasterProcedureManagerHost mpmHost;

    // it is assigned after 'initialized' guard set to true, so should be volatile
    private volatile MasterQuotaManager quotaManager;
    private SpaceQuotaSnapshotNotifier spaceQuotaSnapshotNotifier;
    private QuotaObserverChore quotaObserverChore;
    private SnapshotQuotaObserverChore snapshotQuotaChore;

    private ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
    private WALProcedureStore procedureStore;

    // handle table states
    private TableStateManager tableStateManager;

    private long splitPlanCount;
    private long mergePlanCount;

    /* Handle favored nodes information */
    private FavoredNodesManager favoredNodesManager;

    /**
     * jetty server for master to redirect requests to regionserver infoServer
     */
    private Server masterJettyServer;

    // Determine if we should do normal startup or minimal "single-user" mode with no region
    // servers and no user tables. Useful for repair and recovery of hbase:meta
    private final boolean maintenanceMode;
    static final String MAINTENANCE_MODE = "hbase.master.maintenance_mode";

    public static class RedirectServlet extends HttpServlet {
        private static final long serialVersionUID = 2894774810058302473L;
        private final int regionServerInfoPort;
        private final String regionServerHostname;

        /**
         * @param infoServer that we're trying to send all requests to
         * @param hostname   may be null. if given, will be used for redirects instead of host from client.
         */
        public RedirectServlet(InfoServer infoServer, String hostname) {
            regionServerInfoPort = infoServer.getPort();
            regionServerHostname = hostname;
        }

        @Override
        public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            String redirectHost = regionServerHostname;
            if(redirectHost == null) {
                redirectHost = request.getServerName();
                if(!Addressing.isLocalAddress(InetAddress.getByName(redirectHost))) {
                    LOG.warn(
                            "Couldn't resolve '" + redirectHost + "' as an address local to this node and '" + MASTER_HOSTNAME_KEY + "' is not set; client will get an HTTP 400 response. If " + "your HBase deployment relies on client accessible names that the region server process " + "can't resolve locally, then you should set the previously mentioned configuration variable " + "to an appropriate hostname.");
                    // no sending client provided input back to the client, so the goal host is just in the logs.
                    response.sendError(400,
                            "Request was to a host that I can't resolve for any of the network interfaces on " + "this node. If this is due to an intermediary such as an HTTP load balancer or other proxy, your HBase " + "administrator can set '" + MASTER_HOSTNAME_KEY + "' to point to the correct hostname.");
                    return;
                }
            }
            // TODO this scheme should come from looking at the scheme registered in the infoserver's http server for the
            // host and port we're using, but it's buried way too deep to do that ATM.
            String redirectUrl = request.getScheme() + "://" + redirectHost + ":" + regionServerInfoPort + request.getRequestURI();
            response.sendRedirect(redirectUrl);
        }
    }

    /**
     * Initializes the HMaster. The steps are as follows:
     * <p>
     * <ol>
     * <li>Initialize the local HRegionServer
     * <li>Start the ActiveMasterManager.
     * </ol>
     * <p>
     * Remaining steps of initialization occur in
     * {@link #finishActiveMasterInitialization(MonitoredTask)} after the master becomes the
     * active one.
     *
     * TODO_MA  class HMaster extends HRegionserver
     */
    public HMaster(final Configuration conf) throws IOException, KeeperException {
        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 构造 HMaster 对象实例的时候，执行的初始化代码。因为 HRegionServer 是 HMaster 的父类。
         *   1、创建 RPCServer
         *   2、最重要的操作：初始化一个 ZKWatcher 类型的 zookeeper 实例，用来初始化创建一些 znode 节点
         *   3、启动 RPCServer
         *   4、创建 ActiveMasterManager  用来选举和管理hmaster的状态
         *
         *   hmaster除了调用自己的构造方法，也会调用 父类hregionserver的构造方法
         */
        super(conf);

        TraceUtil.initTracer(conf);
        try {

            // TODO_MA 注释：hbase.master.maintenance_mode
            if(conf.getBoolean(MAINTENANCE_MODE, false)) {
                LOG.info("Detected {}=true via configuration.", MAINTENANCE_MODE);
                maintenanceMode = true;
            } else if(Boolean.getBoolean(MAINTENANCE_MODE)) {
                LOG.info("Detected {}=true via environment variables.", MAINTENANCE_MODE);
                maintenanceMode = true;
            } else {
                maintenanceMode = false;
            }

            // TODO_MA 注释：存放来自 region server 的 fatal error 信息，超过 buffer 大小后会自动清理
            // TODO_MA 注释： HBase 在 HDFS 的根路径是什么
            // TODO_MA 注释： hbase.cluster.distributed 配置成 True
            this.rsFatals = new MemoryBoundedLogMessageBuffer(conf.getLong("hbase.master.buffer.for.rs.fatals", 1 * 1024 * 1024));
            LOG.info("hbase.rootdir=" + getRootDir() + ", hbase.cluster.distributed=" + this.conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, false));

            // Disable usage of meta replicas in the master
            this.conf.setBoolean(HConstants.USE_META_REPLICAS, false);

            // TODO_MA 注释：初始化 Replication 信息
            decorateMasterConfiguration(this.conf);

            // Hack! Maps DFSClient => Master for logs.  HDFS made this
            // config param for task trackers, but we can piggyback off of it.
            if(this.conf.get("mapreduce.task.attempt.id") == null) {
                this.conf.set("mapreduce.task.attempt.id", "hb_m_" + this.serverName.toString());
            }

            this.metricsMaster = new MetricsMaster(new MetricsMasterWrapperImpl(this));

            // preload table descriptor at startup
            this.preLoadTableDescriptors = conf.getBoolean("hbase.master.preload.tabledescriptors", true);

            this.maxBlancingTime = getMaxBalancingTime();
            this.maxRitPercent = conf
                    .getDouble(HConstants.HBASE_MASTER_BALANCER_MAX_RIT_PERCENT, HConstants.DEFAULT_HBASE_MASTER_BALANCER_MAX_RIT_PERCENT);

            // Do we publish the status?
            boolean shouldPublish = conf.getBoolean(HConstants.STATUS_PUBLISHED, HConstants.STATUS_PUBLISHED_DEFAULT);
            Class<? extends ClusterStatusPublisher.Publisher> publisherClass = conf
                    .getClass(ClusterStatusPublisher.STATUS_PUBLISHER_CLASS, ClusterStatusPublisher.DEFAULT_STATUS_PUBLISHER_CLASS,
                            ClusterStatusPublisher.Publisher.class);

            if(shouldPublish) {
                if(publisherClass == null) {
                    LOG.warn(
                            HConstants.STATUS_PUBLISHED + " is true, but " + ClusterStatusPublisher.DEFAULT_STATUS_PUBLISHER_CLASS + " is not set - not publishing status");
                } else {

                    /********
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *   注释：
                     */
                    clusterStatusPublisherChore = new ClusterStatusPublisher(this, conf, publisherClass);
                    getChoreService().scheduleChore(clusterStatusPublisherChore);
                }
            }

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 创建 activeMasterManager
             *   帮助 HMaster 去选举  Active 角色
             *   监控 HMaster的状态，也要去监控 zookeper 集群中的对应的  active master 的znode
             */
            // Some unit tests don't need a cluster, so no zookeeper at all
            if(!conf.getBoolean("hbase.testing.nocluster", false)) {
                this.activeMasterManager = new ActiveMasterManager(zooKeeper, this.serverName, this);
            } else {
                this.activeMasterManager = null;
            }

        } catch(Throwable t) {
            // Make sure we log the exception. HMaster is often started via reflection and the
            // cause of failed startup is lost.
            LOG.error("Failed construction of Master", t);
            throw t;
        }
    }

    @Override
    protected String getUseThisHostnameInstead(Configuration conf) {
        return conf.get(MASTER_HOSTNAME_KEY);
    }

    // Main run loop. Calls through to the regionserver run loop AFTER becoming active Master; will block in here until then.
    @Override
    public void run() {
        try {
            if(!conf.getBoolean("hbase.testing.nocluster", false)) {
                Threads.setDaemonThreadRunning(new Thread(() -> {
                    try {

                        /********
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *   注释： 启动 Web 服务
                         */
                        int infoPort = putUpJettyServer();

                        /********
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *   注释：重要的方法
                         */
                        startActiveMasterManager(infoPort);

                    } catch(Throwable t) {
                        // Make sure we log the exception.
                        String error = "Failed to become Active Master";
                        LOG.error(error, t);

                        /********
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *   注释： 如果启动报错，则取消启动
                         */
                        // Abort should have been called already.
                        if(!isAborted()) {
                            abort(error, t);
                        }
                    }
                }), getName() + ":becomeActiveMaster");
            }

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释：
             */
            // Fall in here even if we have been aborted.
            // Need to run the shutdown services and the super run call will do this for us.
            super.run();

        } finally {
            if(this.clusterSchemaService != null) {
                // If on way out, then we are no longer active master.
                this.clusterSchemaService.stopAsync();
                try {
                    this.clusterSchemaService.awaitTerminated(
                            getConfiguration().getInt(HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS, DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS),
                            TimeUnit.SECONDS);
                } catch(TimeoutException te) {
                    LOG.warn("Failed shutdown of clusterSchemaService", te);
                }
            }
            this.activeMaster = false;
        }
    }

    // return the actual infoPort, -1 means disable info server.
    private int putUpJettyServer() throws IOException {
        if(!conf.getBoolean("hbase.master.infoserver.redirect", true)) {
            return -1;
        }

        // TODO_MA 注释： HBase 的 Http 端口：16010
        final int infoPort = conf.getInt("hbase.master.info.port.orig", HConstants.DEFAULT_MASTER_INFOPORT);
        // -1 is for disabling info server, so no redirecting
        if(infoPort < 0 || infoServer == null) {
            return -1;
        }
        if(infoPort == infoServer.getPort()) {
            return infoPort;
        }

        // TODO_MA 注释： HBase 的 HttpServer 的地址，如果配置的不是本机地址，则直接报错退出
        final String addr = conf.get("hbase.master.info.bindAddress", "0.0.0.0");
        if(!Addressing.isLocalAddress(InetAddress.getByName(addr))) {
            String msg = "Failed to start redirecting jetty server. Address " + addr + " does not belong to this host. Correct configuration parameter: " + "hbase.master.info.bindAddress";
            LOG.error(msg);
            throw new IOException(msg);
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        // TODO I'm pretty sure we could just add another binding to the InfoServer run by
        // the RegionServer and have it run the RedirectServlet instead of standing up a second entire stack here.
        masterJettyServer = new Server();

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 绑定主机名称，和端口号
         */
        final ServerConnector connector = new ServerConnector(masterJettyServer);
        connector.setHost(addr);
        connector.setPort(infoPort);
        masterJettyServer.addConnector(connector);
        masterJettyServer.setStopAtShutdown(true);

        final String redirectHostname = StringUtils.isBlank(useThisHostnameInstead) ? null : useThisHostnameInstead;

        // TODO_MA 注释：绑定一个 Servlet，用来处理 request
        final RedirectServlet redirect = new RedirectServlet(infoServer, redirectHostname);

        final WebAppContext context = new WebAppContext(null, "/", null, null, null, null, WebAppContext.NO_SESSIONS);
        context.addServlet(new ServletHolder(redirect), "/*");
        context.setServer(masterJettyServer);

        try {
            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 启动 Server
             */
            masterJettyServer.start();

        } catch(Exception e) {
            throw new IOException("Failed to start redirecting jetty server", e);
        }
        return connector.getLocalPort();
    }

    @Override
    protected Function<TableDescriptorBuilder, TableDescriptorBuilder> getMetaTableObserver() {
        return builder -> builder.setRegionReplication(conf.getInt(HConstants.META_REPLICAS_NUM, HConstants.DEFAULT_META_REPLICA_NUM));
    }

    /**
     * For compatibility, if failed with regionserver credentials, try the master one
     */
    @Override
    protected void login(UserProvider user, String host) throws IOException {
        try {
            super.login(user, host);
        } catch(IOException ie) {
            user.login(SecurityConstants.MASTER_KRB_KEYTAB_FILE, SecurityConstants.MASTER_KRB_PRINCIPAL, host);
        }
    }

    /**
     * If configured to put regions on active master, wait till a backup master becomes active.
     * Otherwise, loop till the server is stopped or aborted.
     */
    @Override
    protected void waitForMasterActive() {
        if(maintenanceMode) {
            return;
        }
        boolean tablesOnMaster = LoadBalancer.isTablesOnMaster(conf);
        while(!(tablesOnMaster && activeMaster) && !isStopped() && !isAborted()) {
            sleeper.sleep();
        }
    }

    @VisibleForTesting
    public MasterRpcServices getMasterRpcServices() {
        return (MasterRpcServices) rpcServices;
    }

    public boolean balanceSwitch(final boolean b) throws IOException {
        return getMasterRpcServices().switchBalancer(b, BalanceSwitchMode.ASYNC);
    }

    @Override
    protected String getProcessName() {
        return MASTER;
    }

    @Override
    protected boolean canCreateBaseZNode() {
        return true;
    }

    @Override
    protected boolean canUpdateTableDescriptor() {
        return true;
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
        return new MasterRpcServices(this);
    }

    @Override
    protected void configureInfoServer() {
        infoServer.addUnprivilegedServlet("master-status", "/master-status", MasterStatusServlet.class);
        infoServer.setAttribute(MASTER, this);
        if(LoadBalancer.isTablesOnMaster(conf)) {
            super.configureInfoServer();
        }
    }

    @Override
    protected Class<? extends HttpServlet> getDumpServlet() {
        return MasterDumpServlet.class;
    }

    @Override
    public MetricsMaster getMasterMetrics() {
        return metricsMaster;
    }

    /**
     * <p>
     * Initialize all ZK based system trackers. But do not include {@link RegionServerTracker}, it
     * should have already been initialized along with {@link ServerManager}.
     * </p>
     * <p>
     * Will be overridden in tests.
     * </p>
     */
    @VisibleForTesting
    protected void initializeZKBasedSystemTrackers() throws IOException, InterruptedException, KeeperException, ReplicationException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：默认的 负载均衡器：StochasticLoadBalancer
         */
        this.balancer = LoadBalancerFactory.getLoadBalancer(conf);

        this.normalizer = RegionNormalizerFactory.getRegionNormalizer(conf);
        this.normalizer.setMasterServices(this);
        this.normalizer.setMasterRpcServices((MasterRpcServices) rpcServices);


        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 启动负载均衡跟踪器：LoadBalancerTracker
         */
        this.loadBalancerTracker = new LoadBalancerTracker(zooKeeper, this);
        this.loadBalancerTracker.start();


        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        this.regionNormalizerTracker = new RegionNormalizerTracker(zooKeeper, this);
        this.regionNormalizerTracker.start();


        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        this.splitOrMergeTracker = new SplitOrMergeTracker(zooKeeper, conf, this);
        this.splitOrMergeTracker.start();


        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        this.replicationPeerManager = ReplicationPeerManager.create(zooKeeper, conf);


        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        this.drainingServerTracker = new DrainingServerTracker(zooKeeper, this, this.serverManager);
        this.drainingServerTracker.start();

        String clientQuorumServers = conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM);
        boolean clientZkObserverMode = conf.getBoolean(HConstants.CLIENT_ZOOKEEPER_OBSERVER_MODE, HConstants.DEFAULT_CLIENT_ZOOKEEPER_OBSERVER_MODE);
        if(clientQuorumServers != null && !clientZkObserverMode) {
            // we need to take care of the ZK information synchronization
            // if given client ZK are not observer nodes
            ZKWatcher clientZkWatcher = new ZKWatcher(conf, getProcessName() + ":" + rpcServices.getSocketAddress().getPort() + "-clientZK", this,
                    false, true);
            this.metaLocationSyncer = new MetaLocationSyncer(zooKeeper, clientZkWatcher, this);
            this.metaLocationSyncer.start();
            this.masterAddressSyncer = new MasterAddressSyncer(zooKeeper, clientZkWatcher, this);
            this.masterAddressSyncer.start();
            // set cluster id is a one-go effort
            ZKClusterId.setClusterId(clientZkWatcher, fileSystemManager.getClusterId());
        }

        // Set the cluster as up.  If new RSs, they'll be waiting on this before
        // going ahead with their startup.
        boolean wasUp = this.clusterStatusTracker.isClusterUp();
        if(!wasUp)
            this.clusterStatusTracker.setClusterUp();

        LOG.info("Active/primary master=" + this.serverName + ", sessionid=0x" + Long
                .toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()) + ", setting cluster-up flag (Was=" + wasUp + ")");

        // create/initialize the snapshot manager and other procedure managers
        this.snapshotManager = new SnapshotManager();
        this.mpmHost = new MasterProcedureManagerHost();
        this.mpmHost.register(this.snapshotManager);
        this.mpmHost.register(new MasterFlushTableProcedureManager());
        this.mpmHost.loadProcedures(conf);
        this.mpmHost.initialize(this, this.metricsMaster);
    }

    private static final ImmutableSet<Class<? extends Procedure>> UNSUPPORTED_PROCEDURES = ImmutableSet
            .of(RecoverMetaProcedure.class, AssignProcedure.class, UnassignProcedure.class, MoveRegionProcedure.class);

    /**
     * In HBASE-20811, we have introduced a new TRSP to assign/unassign/move regions, and it is
     * incompatible with the old AssignProcedure/UnassignProcedure/MoveRegionProcedure. So we need to
     * make sure that there are none these procedures when upgrading. If there are, the master will
     * quit, you need to go back to the old version to finish these procedures first before upgrading.
     */
    private void checkUnsupportedProcedure(Map<Class<? extends Procedure>, List<Procedure<MasterProcedureEnv>>> procsByType) throws HBaseIOException {
        // Confirm that we do not have unfinished assign/unassign related procedures. It is not easy to
        // support both the old assign/unassign procedures and the new TransitRegionStateProcedure as
        // there will be conflict in the code for AM. We should finish all these procedures before
        // upgrading.
        for(Class<? extends Procedure> clazz : UNSUPPORTED_PROCEDURES) {
            List<Procedure<MasterProcedureEnv>> procs = procsByType.get(clazz);
            if(procs != null) {
                LOG.error(
                        "Unsupported procedure type {} found, please rollback your master to the old" + " version to finish them, and then try to upgrade again. The full procedure list: {}",
                        clazz, procs);
                throw new HBaseIOException("Unsupported procedure type " + clazz + " found");
            }
        }
        // A special check for SCP, as we do not support RecoverMetaProcedure any more so we need to
        // make sure that no one will try to schedule it but SCP does have a state which will schedule
        // it.
        if(procsByType.getOrDefault(ServerCrashProcedure.class, Collections.emptyList()).stream().map(p -> (ServerCrashProcedure) p)
                .anyMatch(ServerCrashProcedure::isInRecoverMetaState)) {
            LOG.error(
                    "At least one ServerCrashProcedure is going to schedule a RecoverMetaProcedure," + " which is not supported any more. Please rollback your master to the old version to" + " finish them, and then try to upgrade again.");
            throw new HBaseIOException("Unsupported procedure state found for ServerCrashProcedure");
        }
    }

    // Will be overriden in test to inject customized AssignmentManager
    @VisibleForTesting
    protected AssignmentManager createAssignmentManager(MasterServices master) {
        return new AssignmentManager(master);
    }

    /**
     * Finish initialization of HMaster after becoming the primary master.
     * 成为 active master 后，完成 HMaster 的初始化
     *
     * The startup order is a bit complicated but very important, do not change it unless you know what you are doing.
     * 启动顺序有点复杂，但非常重要，除非您知道自己在做什么，否则请不要更改它。
     *
     * <li>Initialize file system based components - file system manager, wal manager, table descriptors, etc</li>
     * 初始化基于文件系统的组件 - fileSystemManager，walManager，tableDescriptor 等
     *
     * <li>Publish cluster id</li>
     * <li>Here comes the most complicated part - initialize server manager, assignment manager and region server tracker
     * 这是最复杂的部分 - 初始化 serverManager，assignmentManager, regionserverTracker 三个重要的组件
     *
     * <ol type='i'>
     * <li>Create server manager</li>
     * <li>Create procedure executor, load the procedures, but do not start workers. We will start it
     * later after we finish scheduling SCPs to avoid scheduling duplicated SCPs for the same server</li>
     * 创建 procedureExecutor，加载各种 procedure，但不启动工作程序。我们将在完成调度SCP之后开始它，以避免为同一服务器调度重复的SCP。
     *
     * <li>Create assignment manager and start it, load the meta region state, but do not load data from meta region</li>
     * 创建 assignmentManager 并启动它，加载 meta region 的状态，但是不从 meta 区域加载数据
     *
     * <li>Start region server tracker, construct the online servers set and find out dead servers and schedule SCP for them.
     * The online servers will be constructed by scanning zk, and we will also scan the wal directory to find out
     * possible live region servers, and the differences between these two sets are the dead servers</li>
     * 启动 regionServerTracker，构建在 online servers 并找出 dead servers，并为它们安排 SCP。
     * online servers 将通过 scan zookeeper 来构建，并且我们还将 scan wal directory 以查找可能的 live region servers，
     * 并且这两组 regionserver 集合之间的区别是 dead servers。
     *
     * </ol>
     * </li>
     * <li>If this is a new deploy, schedule a InitMetaProcedure to initialize meta</li>
     * 如果这是新部署，请安排一个 InitMetaProcedure 初始化 meta 表
     *
     * <li>Start necessary service threads - balancer, catalog janior, executor services, and also the procedure executor, etc.
     * Notice that the balancer must be created first as assignment manager may use it when assigning regions.</li>
     * 启动必要的服务线程 - balancer，catalogjanior，executorServices 以及 procedureExecutor 等。
     * 请注意，必须首先创建 balancer ，因为 assignmentManager可能在 assigning regions 使用它。
     *
     * <li>Wait for meta to be initialized if necesssary, start table state manager.</li>
     * 等待必要的meta初始化，启动 tableStateManager。
     *
     * <li>Wait for enough region servers to check-in</li>
     * 等待足够的 regionserver 签入、签到
     *
     * <li>Let assignment manager load data from meta and construct region states</li>
     * 让 assignmentManager 从 meta 加载数据并构造 region states
     *
     * <li>Start all other things such as chore services, etc</li>
     * 开始其他所有事情，例如杂务服务等
     *
     * </ol>
     * <p/>
     * Notice that now we will not schedule a special procedure to make meta online(unless the first
     * time where meta has not been created yet), we will rely on SCP to bring meta online.
     * 请注意，现在我们将不再安排特殊的程序来使元数据在线（除非第一次还没有创建元数据），我们将依靠SCP使元数据在线。
     */
    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释： 启动和初始化一堆服务组件
     */
    private void finishActiveMasterInitialization(
            MonitoredTask status) throws IOException, InterruptedException, KeeperException, ReplicationException {
        /*
         * We are active master now... go initialize components we need to run.
         */
        status.setStatus("Initializing Master file system");

        this.masterActiveTime = System.currentTimeMillis();
        // TODO: Do this using Dependency Injection, using PicoContainer, Guice or Spring.

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 ChunkCreator： MemStoreLAB 主要是为了解决 memstoreflush 的内存碎片问题，而导致的 javagc。
         *
         *   正常来说，memstore 的底层实现是一个 链表。每个节点其实就是一个 KeyValue 对象
         *   如果插入一条数据，一个key-value 就会在 HRegionserver 的堆内存当中，申请一块内存区域，用来存储当前
         *   这个 key-value的真实数据，再插入一个数据，同样也是申请一块内存区域插入到链表中的
         *   你多次插入的 KeyValue 事实上在内存中是不连续的。而且，这些对象将来会都被进化到 老年代
         *   1、会产生内存碎片，  2，大量 key-value 会提升到老年代， full GC 压力大
         *
         *   优化方案：Chunk   一个块
         *      插入一个cell: 就只是申请一块能存储当前 cell 对象的内存就够了
         *      我一口气申请 2M 的内存, 这 2M 是连续的。变成一个对象，插入到链表中。
         *
         *      这句代码就是初始化了一个 ChunkCreator 实例  该实例的作用。就是用来创建 chunk 的！
         */
        // Only initialize the MemStoreLAB when master carry table
        if(LoadBalancer.isTablesOnMaster(conf)) {
            initializeMemStoreChunkCreator();
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 MasterFileSystem
         */
        this.fileSystemManager = new MasterFileSystem(conf);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 MasterWalManager
         */
        this.walManager = new MasterWalManager(this);

        // enable table descriptors cache
        this.tableDescriptors.setCacheOn();

        // warm-up HTDs cache on master initialization
        if(preLoadTableDescriptors) {
            status.setStatus("Pre-loading table descriptors");
            this.tableDescriptors.getAll();
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 注册 clusterID 放在 zookeeper 中
         */
        // Publish cluster ID; set it in Master too. The superclass RegionServer does this later but
        // only after it has checked in with the Master. At least a few tests ask Master for clusterId
        // before it has called its run method and before RegionServer has done the reportForDuty.
        ClusterId clusterId = fileSystemManager.getClusterId();
        status.setStatus("Publishing Cluster ID " + clusterId + " in ZooKeeper");
        ZKClusterId.setClusterId(this.zooKeeper, fileSystemManager.getClusterId());
        this.clusterId = clusterId.toString();

        // TODO_MA 注释：假设做了 hbase1.x 到  hbase2.x 的升级。
        // TODO_MA 注释：往HDFS中写一个锁文件，为了防止，HBase1.x 和 HBase2.x 的冲突问题
        // Precaution. Put in place the old hbck1 lock file to fence out old hbase1s running their
        // hbck1s against an hbase2 cluster; it could do damage.
        // To skip this behavior, set hbase.write.hbck1.lock.file to false.
        if(this.conf.getBoolean("hbase.write.hbck1.lock.file", true)) {
            HBaseFsck.checkAndMarkRunningHbck(this.conf, HBaseFsck.createLockRetryCounterFactory(this.conf).create());
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：初始化 ServerManager，管理RegionServer。
         */
        status.setStatus("Initialize ServerManager and schedule SCP for crash servers");
        this.serverManager = createServerManager(this);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 SplitWALManager
         */
        if(!conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK, DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)) {
            this.splitWALManager = new SplitWALManager(this);
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 ProcedureExecutor
         *   将来你的各种请求，比如 createTable, listTable, put, get 等等这些用户请求，都是被封装成一个个的
         *   Procedure 来执行的。 放在线程池里面执行。
         *   ProcedureExecutor 命令执行器！！！
         */
        createProcedureExecutor();

        @SuppressWarnings("rawtypes") Map<Class<? extends Procedure>, List<Procedure<MasterProcedureEnv>>> procsByType = procedureExecutor
                .getActiveProceduresNoCopy().stream().collect(Collectors.groupingBy(p -> p.getClass()));
        checkUnsupportedProcedure(procsByType);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 AssignmentManager
         *   用来帮助 HMaster 去分配那些 region 给那些 regionsever 去管理
         *   默认的实现就是：AssignmentManager
         */
        // Create Assignment Manager
        this.assignmentManager = createAssignmentManager(this);
        this.assignmentManager.start();

        // TODO: TRSP can perform as the sub procedure for other procedures, so even if it is marked as
        // completed, it could still be in the procedure list. This is a bit strange but is another
        // story, need to verify the implementation for ProcedureExecutor and ProcedureStore.
        List<TransitRegionStateProcedure> ritList = procsByType.getOrDefault(TransitRegionStateProcedure.class, Collections.emptyList()).stream()
                .filter(p -> !p.isFinished()).map(p -> (TransitRegionStateProcedure) p).collect(Collectors.toList());
        this.assignmentManager.setupRIT(ritList);
        // RIT 属于调优的问题。

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 RegionServerTracker
         */
        // Start RegionServerTracker with listing of servers found with exiting SCPs -- these should
        // be registered in the deadServers set -- and with the list of servernames out on the
        // filesystem that COULD BE 'alive' (we'll schedule SCPs for each and let SCP figure it out).
        // We also pass dirs that are already 'splitting'... so we can do some checks down in tracker.
        // TODO: Generate the splitting and live Set in one pass instead of two as we currently do.
        this.regionServerTracker = new RegionServerTracker(zooKeeper, this, this.serverManager);
        this.regionServerTracker
                .start(procsByType.getOrDefault(ServerCrashProcedure.class, Collections.emptyList()).stream().map(p -> (ServerCrashProcedure) p)
                                .map(p -> p.getServerName()).collect(Collectors.toSet()), walManager.getLiveServersFromWALDir(),
                        walManager.getSplittingServersFromWALDir());

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 TableStateManager， 管理管理表状态。
         */
        // This manager will be started AFTER hbase:meta is confirmed on line.
        // hbase.mirror.table.state.to.zookeeper is so hbase1 clients can connect.
        // They read table state from zookeeper while hbase2 reads it from hbase:meta.
        // Disable if no hbase1 clients.
        this.tableStateManager = this.conf.getBoolean(MirroringTableStateManager.MIRROR_TABLE_STATE_TO_ZK_KEY, true) ? new MirroringTableStateManager(
                this) : new TableStateManager(this);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化所有基于ZK的tracker
         */
        status.setStatus("Initializing ZK system trackers");
        initializeZKBasedSystemTrackers();

        // Set ourselves as active Master now our claim has succeeded up in zk.
        this.activeMaster = true;

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 启动 Zombie master detector
         */
        // Start the Zombie master detector after setting master as active, see HBASE-21535
        Thread zombieDetector = new Thread(new InitializationMonitor(this), "ActiveMasterInitializationMonitor-" + System.currentTimeMillis());
        zombieDetector.setDaemon(true);
        zombieDetector.start();

        // This is for backwards compatibility
        // See HBASE-11393
        status.setStatus("Update TableCFs node in ZNode");
        ReplicationPeerConfigUpgrader tableCFsUpdater = new ReplicationPeerConfigUpgrader(zooKeeper, conf);
        tableCFsUpdater.copyTableCFs();

        if(!maintenanceMode) {
            // Add the Observer to delete quotas on table deletion before starting all CPs by
            // default with quota support, avoiding if user specifically asks to not load this Observer.
            if(QuotaUtil.isQuotaEnabled(conf)) {
                updateConfigurationForQuotasObserver(conf);
            }

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 初始化 Master 端的 Coprocessor，为 master 提供协处理框架和环境
             */
            // initialize master side coprocessors before we start handling requests
            status.setStatus("Initializing master coprocessors");
            this.cpHost = new MasterCoprocessorHost(this, this.conf);
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 MetaTable，如果这是第一次部署启动
         */
        // Checking if meta needs initializing.
        status.setStatus("Initializing meta table if this is a new deploy");
        InitMetaProcedure initMetaProc = null;

        // Print out state of hbase:meta on startup; helps debugging.
        RegionState rs = this.assignmentManager.getRegionStates().getRegionState(RegionInfoBuilder.FIRST_META_REGIONINFO);
        LOG.info("hbase:meta {}", rs);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         *   1、hbase-0.96x 以前：  .meta.  -root-
         *   2、hbase-0.96x 以后： 就只有 meta 表了。
         *   如果 habse集群是第一次启动， 则会初始化 meta 表
         */

        // TODO_MA 注释：第一次的话，需要初始化 meta 表
        if(rs.isOffline()) {
            Optional<InitMetaProcedure> optProc = procedureExecutor.getProcedures().stream().filter(p -> p instanceof InitMetaProcedure)
                    .map(o -> (InitMetaProcedure) o).findAny();
            initMetaProc = optProc.orElseGet(() -> {

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 提交一个 初始化 meta 表的请求
                 */
                // schedule an init meta procedure if meta has not been deployed yet
                InitMetaProcedure temp = new InitMetaProcedure();
                procedureExecutor.submitProcedure(temp);
                return temp;
            });
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 FavoredNodesManager
         */
        if(this.balancer instanceof FavoredNodesPromoter) {
            favoredNodesManager = new FavoredNodesManager(this);
        }


        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 balancer
         */
        // initialize load balancer
        this.balancer.setMasterServices(this);
        this.balancer.setClusterMetrics(getClusterMetricsWithoutCoprocessor());
        this.balancer.initialize();

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 启动各种服务线程
         */
        // start up all service threads.
        status.setStatus("Initializing master service threads");
        startServiceThreads();

        // wait meta to be initialized after we start procedure executor
        if(initMetaProc != null) {
            initMetaProc.await();
        }
        // Wake up this server to check in
        sleeper.skipSleepCycle();

        // Wait for region servers to report in.
        // With this as part of master initialization, it precludes our being able to start a single
        // server that is both Master and RegionServer. Needs more thought. TODO.
        String statusStr = "Wait for region servers to report in";
        status.setStatus(statusStr);
        LOG.info(Objects.toString(status));

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：等待regionserver上线
         */
        waitForRegionServers(status);

        // Check if master is shutting down because issue initializing regionservers or balancer.
        if(isStopped()) {
            return;
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 等 HBASE:META 上线
         */
        status.setStatus("Starting assignment manager");
        // FIRST HBASE:META READ!!!!
        // The below cannot make progress w/o hbase:meta being online.
        // This is the FIRST attempt at going to hbase:meta. Meta on-lining is going on in background
        // as procedures run -- in particular SCPs for crashed servers... One should put up hbase:meta
        // if it is down. It may take a while to come online. So, wait here until meta if for sure
        // available. That's what waitForMetaOnline does.
        if(!waitForMetaOnline()) {
            return;
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 上线 meta 表
         */
        this.assignmentManager.joinCluster();

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        // The below depends on hbase:meta being online.
        this.tableStateManager.start();

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 处理 region 的问题
         *   在通过 hbase-1.x 数据集启动此 hbase-2.x 的情况下，
         *   必须在 tablestatemanager 启动后发生以下情况。
         *   tablestatemanager 运行迁移，这是其“开始”表状态从Zookeeper迁移到hbase：meta的一部分。
         *   此迁移需要完成，然后再执行下一步处理脱机区域，否则它将无法读取混淆主启动的表状态（未分配名称空间表等）。
         */
        // Below has to happen after tablestatemanager has started in the case where this hbase-2.x
        // is being started over an hbase-1.x dataset. tablestatemanager runs a migration as part
        // of its 'start' moving table state from zookeeper to hbase:meta. This migration needs to
        // complete before we do this next step processing offline regions else it fails reading
        // table states messing up master launch (namespace table, etc., are not assigned).
        this.assignmentManager.processOfflineRegions();

        // Initialize after meta is up as below scans meta
        if(favoredNodesManager != null && !maintenanceMode) {
            SnapshotOfRegionAssignmentFromMeta snapshotOfRegionAssignment = new SnapshotOfRegionAssignmentFromMeta(getConnection());
            snapshotOfRegionAssignment.initialize();
            favoredNodesManager.initialize(snapshotOfRegionAssignment);
        }

        // set cluster status again after user regions are assigned
        this.balancer.setClusterMetrics(getClusterMetricsWithoutCoprocessor());

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 当 meta上线了，当regions被分配完毕了，然后启动 balancer and meta catalog janitor
         */
        // Start balancer and meta catalog janitor after meta and regions have been assigned.
        status.setStatus("Starting balancer and catalog janitor");
        this.clusterStatusChore = new ClusterStatusChore(this, balancer);
        getChoreService().scheduleChore(clusterStatusChore);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         *   1、hbase.balancer.period = 默认5分钟
         *   2、调度起来
         */
        this.balancerChore = new BalancerChore(this);
        getChoreService().scheduleChore(balancerChore);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：　启动其他各种定时调度
         */
        this.normalizerChore = new RegionNormalizerChore(this);
        getChoreService().scheduleChore(normalizerChore);
        this.catalogJanitorChore = new CatalogJanitor(this);
        getChoreService().scheduleChore(catalogJanitorChore);
        this.hbckChore = new HbckChore(this);
        getChoreService().scheduleChore(hbckChore);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 等待 Namespace 表上线
         *
         *   两张系统表：
         *   1、meta
         *   2、namespace
         */
        // NAMESPACE READ!!!!
        // Here we expect hbase:namespace to be online. See inside initClusterSchemaService.
        // TODO: Fix this. Namespace is a pain being a sort-of system table. Fold it in to hbase:meta.
        // isNamespace does like isMeta and waits until namespace is onlined before allowing progress.
        if(!waitForNamespaceOnline()) {
            return;
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        status.setStatus("Starting cluster schema service");
        initClusterSchemaService();

        if(this.cpHost != null) {
            try {
                this.cpHost.preMasterInitialization();
            } catch(IOException e) {
                LOG.error("Coprocessor preMasterInitialization() hook failed", e);
            }
        }

        status.markComplete("Initialization successful");
        LOG.info(String.format("Master has completed initialization %.3fsec", (System.currentTimeMillis() - masterActiveTime) / 1000.0f));

        this.masterFinishedInitializationTime = System.currentTimeMillis();
        configurationManager.registerObserver(this.balancer);
        configurationManager.registerObserver(this.cleanerPool);
        configurationManager.registerObserver(this.hfileCleaner);
        configurationManager.registerObserver(this.logCleaner);

        // Set master as 'initialized'.
        setInitialized(true);

        if(maintenanceMode) {
            LOG.info("Detected repair mode, skipping final initialization steps.");
            return;
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        assignmentManager.checkIfShouldMoveSystemRegionAsync();

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 MetaBootstrap
         */
        status.setStatus("Assign meta replicas");
        MasterMetaBootstrap metaBootstrap = createMetaBootstrap();
        metaBootstrap.assignMetaReplicas();
        status.setStatus("Starting quota manager");

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 QuotaManager
         */
        initQuotaManager();
        if(QuotaUtil.isQuotaEnabled(conf)) {
            // Create the quota snapshot notifier
            spaceQuotaSnapshotNotifier = createQuotaSnapshotNotifier();
            spaceQuotaSnapshotNotifier.initialize(getClusterConnection());
            this.quotaObserverChore = new QuotaObserverChore(this, getMasterMetrics());
            // Start the chore to read the region FS space reports and act on them
            getChoreService().scheduleChore(quotaObserverChore);

            this.snapshotQuotaChore = new SnapshotQuotaObserverChore(this, getMasterMetrics());
            // Start the chore to read snapshots and add their usage to table/NS quotas
            getChoreService().scheduleChore(snapshotQuotaChore);
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        // clear the dead servers with same host name and port of online server because we are not
        // removing dead server with same hostname and port of rs which is trying to check in before
        // master initialization. See HBASE-5916.
        this.serverManager.clearDeadServersWithSameHostNameAndPortOfOnlineServer();

        // Check and set the znode ACLs if needed in case we are overtaking a non-secure configuration
        status.setStatus("Checking ZNode ACLs");
        zooKeeper.checkAndSetZNodeAcls();

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 初始化 MobCleaner
         */
        status.setStatus("Initializing MOB Cleaner");
        initMobCleaner();

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 提供 Master 协处理器处理环境
         */
        status.setStatus("Calling postStartMaster coprocessors");
        if(this.cpHost != null) {
            // don't let cp initialization errors kill the master
            try {
                this.cpHost.postStartMaster();
            } catch(IOException ioe) {
                LOG.error("Coprocessor postStartMaster() hook failed", ioe);
            }
        }
        zombieDetector.interrupt();

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 当 Master 启动好了之后，该启动 balancer
         */
        /*
         * After master has started up, lets do balancer post startup initialization.
         * Since this runs in activeMasterManager thread, it should be fine.
         */
        long start = System.currentTimeMillis();
        this.balancer.postMasterStartupInitialize();
        if(LOG.isDebugEnabled()) {
            LOG.debug("Balancer post startup initialization complete, took " + ((System.currentTimeMillis() - start) / 1000) + " seconds");
        }
    }

    /**
     * Check hbase:meta is up and ready for reading. For use during Master startup only.
     *
     * @return True if meta is UP and online and startup can progress. Otherwise, meta is not online
     * and we will hold here until operator intervention.
     */
    @VisibleForTesting
    public boolean waitForMetaOnline() throws InterruptedException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        return isRegionOnline(RegionInfoBuilder.FIRST_META_REGIONINFO);
    }

    /**
     * @return True if region is online and scannable else false if an error or shutdown (Otherwise
     * we just block in here holding up all forward-progess).
     */
    private boolean isRegionOnline(RegionInfo ri) throws InterruptedException {
        RetryCounter rc = null;
        while(!isStopped()) {
            RegionState rs = this.assignmentManager.getRegionStates().getRegionState(ri);
            if(rs.isOpened()) {
                if(this.getServerManager().isServerOnline(rs.getServerName())) {
                    return true;
                }
            }
            // Region is not OPEN.
            Optional<Procedure<MasterProcedureEnv>> optProc = this.procedureExecutor.getProcedures().
                    stream().filter(p -> p instanceof ServerCrashProcedure).findAny();
            // TODO: Add a page to refguide on how to do repair. Have this log message point to it.
            // Page will talk about loss of edits, how to schedule at least the meta WAL recovery, and
            // then how to assign including how to break region lock if one held.
            LOG.warn(
                    "{} is NOT online; state={}; ServerCrashProcedures={}. Master startup cannot " + "progress, in holding-pattern until region onlined.",
                    ri.getRegionNameAsString(), rs, optProc.isPresent());
            // Check once-a-minute.
            if(rc == null) {
                rc = new RetryCounterFactory(1000).create();
            }
            Threads.sleep(rc.getBackoffTimeAndIncrementAttempts());
        }
        return false;
    }

    /**
     * Check hbase:namespace table is assigned. If not, startup will hang looking for the ns table
     * (TODO: Fix this! NS should not hold-up startup).
     *
     * @return True if namespace table is up/online.
     */
    @VisibleForTesting
    public boolean waitForNamespaceOnline() throws InterruptedException {
        List<RegionInfo> ris = this.assignmentManager.getRegionStates().getRegionsOfTable(TableName.NAMESPACE_TABLE_NAME);
        if(ris.isEmpty()) {
            // If empty, means we've not assigned the namespace table yet... Just return true so startup
            // continues and the namespace table gets created.
            return true;
        }
        // Else there are namespace regions up in meta. Ensure they are assigned before we go on.
        for(RegionInfo ri : ris) {
            isRegionOnline(ri);
        }
        return true;
    }

    /**
     * Adds the {@code MasterQuotasObserver} to the list of configured Master observers to
     * automatically remove quotas for a table when that table is deleted.
     */
    @VisibleForTesting
    public void updateConfigurationForQuotasObserver(Configuration conf) {
        // We're configured to not delete quotas on table deletion, so we don't need to add the obs.
        if(!conf.getBoolean(MasterQuotasObserver.REMOVE_QUOTA_ON_TABLE_DELETE, MasterQuotasObserver.REMOVE_QUOTA_ON_TABLE_DELETE_DEFAULT)) {
            return;
        }
        String[] masterCoprocs = conf.getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
        final int length = null == masterCoprocs ? 0 : masterCoprocs.length;
        String[] updatedCoprocs = new String[length + 1];
        if(length > 0) {
            System.arraycopy(masterCoprocs, 0, updatedCoprocs, 0, masterCoprocs.length);
        }
        updatedCoprocs[length] = MasterQuotasObserver.class.getName();
        conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, updatedCoprocs);
    }

    private void initMobCleaner() {
        this.expiredMobFileCleanerChore = new ExpiredMobFileCleanerChore(this);
        getChoreService().scheduleChore(expiredMobFileCleanerChore);

        int mobCompactionPeriod = conf.getInt(MobConstants.MOB_COMPACTION_CHORE_PERIOD, MobConstants.DEFAULT_MOB_COMPACTION_CHORE_PERIOD);
        this.mobCompactChore = new MobCompactionChore(this, mobCompactionPeriod);
        getChoreService().scheduleChore(mobCompactChore);
        this.mobCompactThread = new MasterMobCompactionThread(this);
    }

    /**
     * <p>
     * Create a {@link MasterMetaBootstrap} instance.
     * </p>
     * <p>
     * Will be overridden in tests.
     * </p>
     */
    @VisibleForTesting
    protected MasterMetaBootstrap createMetaBootstrap() {
        // We put this out here in a method so can do a Mockito.spy and stub it out
        // w/ a mocked up MasterMetaBootstrap.
        return new MasterMetaBootstrap(this);
    }

    /**
     * <p>
     * Create a {@link ServerManager} instance.
     * </p>
     * <p>
     * Will be overridden in tests.
     * </p>
     */
    @VisibleForTesting
    protected ServerManager createServerManager(final MasterServices master) throws IOException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        // We put this out here in a method so can do a Mockito.spy and stub it out w/ a mocked up ServerManager.
        setupClusterConnection();
        return new ServerManager(master);
    }

    private void waitForRegionServers(final MonitoredTask status) throws IOException, InterruptedException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        this.serverManager.waitForRegionServers(status);
    }

    // Will be overridden in tests
    @VisibleForTesting
    protected void initClusterSchemaService() throws IOException, InterruptedException {
        this.clusterSchemaService = new ClusterSchemaServiceImpl(this);
        this.clusterSchemaService.startAsync();
        try {
            this.clusterSchemaService
                    .awaitRunning(getConfiguration().getInt(HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS, DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS),
                            TimeUnit.SECONDS);
        } catch(TimeoutException toe) {
            throw new IOException("Timedout starting ClusterSchemaService", toe);
        }
    }

    private void initQuotaManager() throws IOException {
        MasterQuotaManager quotaManager = new MasterQuotaManager(this);
        quotaManager.start();
        this.quotaManager = quotaManager;
    }

    private SpaceQuotaSnapshotNotifier createQuotaSnapshotNotifier() {
        SpaceQuotaSnapshotNotifier notifier = SpaceQuotaSnapshotNotifierFactory.getInstance().create(getConfiguration());
        return notifier;
    }

    boolean isCatalogJanitorEnabled() {
        return catalogJanitorChore != null ? catalogJanitorChore.getEnabled() : false;
    }

    boolean isCleanerChoreEnabled() {
        boolean hfileCleanerFlag = true, logCleanerFlag = true;

        if(hfileCleaner != null) {
            hfileCleanerFlag = hfileCleaner.getEnabled();
        }

        if(logCleaner != null) {
            logCleanerFlag = logCleaner.getEnabled();
        }

        return (hfileCleanerFlag && logCleanerFlag);
    }

    @Override
    public ServerManager getServerManager() {
        return this.serverManager;
    }

    @Override
    public MasterFileSystem getMasterFileSystem() {
        return this.fileSystemManager;
    }

    @Override
    public MasterWalManager getMasterWalManager() {
        return this.walManager;
    }

    @Override
    public SplitWALManager getSplitWALManager() {
        return splitWALManager;
    }

    @Override
    public TableStateManager getTableStateManager() {
        return tableStateManager;
    }

    /*
     * Start up all services. If any of these threads gets an unhandled exception
     * then they just die with a logged message.  This should be fine because
     * in general, we do not expect the master to get such unhandled exceptions
     *  as OOMEs; it should be lightly loaded. See what HRegionServer does if
     *  need to install an unexpected exception handler.
     */
    private void startServiceThreads() throws IOException {
        // Start the executor service pools
        this.executorService.startExecutorService(ExecutorType.MASTER_OPEN_REGION,
                conf.getInt(HConstants.MASTER_OPEN_REGION_THREADS, HConstants.MASTER_OPEN_REGION_THREADS_DEFAULT));
        this.executorService.startExecutorService(ExecutorType.MASTER_CLOSE_REGION,
                conf.getInt(HConstants.MASTER_CLOSE_REGION_THREADS, HConstants.MASTER_CLOSE_REGION_THREADS_DEFAULT));
        this.executorService.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS,
                conf.getInt(HConstants.MASTER_SERVER_OPERATIONS_THREADS, HConstants.MASTER_SERVER_OPERATIONS_THREADS_DEFAULT));
        this.executorService.startExecutorService(ExecutorType.MASTER_META_SERVER_OPERATIONS,
                conf.getInt(HConstants.MASTER_META_SERVER_OPERATIONS_THREADS, HConstants.MASTER_META_SERVER_OPERATIONS_THREADS_DEFAULT));
        this.executorService.startExecutorService(ExecutorType.M_LOG_REPLAY_OPS,
                conf.getInt(HConstants.MASTER_LOG_REPLAY_OPS_THREADS, HConstants.MASTER_LOG_REPLAY_OPS_THREADS_DEFAULT));
        this.executorService.startExecutorService(ExecutorType.MASTER_SNAPSHOT_OPERATIONS,
                conf.getInt(SnapshotManager.SNAPSHOT_POOL_THREADS_KEY, SnapshotManager.SNAPSHOT_POOL_THREADS_DEFAULT));

        // We depend on there being only one instance of this executor running
        // at a time.  To do concurrency, would need fencing of enable/disable of
        // tables.
        // Any time changing this maxThreads to > 1, pls see the comment at
        // AccessController#postCompletedCreateTableAction
        this.executorService.startExecutorService(ExecutorType.MASTER_TABLE_OPERATIONS, 1);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 启动各种执行任务的 Executor
         */
        startProcedureExecutor();

        // Create cleaner thread pool
        cleanerPool = new DirScanPool(conf);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 定时任务：LogCleaner
         */
        // Start log cleaner thread
        int cleanerInterval = conf.getInt("hbase.master.cleaner.interval", 600 * 1000);
        this.logCleaner = new LogCleaner(cleanerInterval, this, conf, getMasterWalManager().getFileSystem(), getMasterWalManager().getOldLogDir(),
                cleanerPool);
        getChoreService().scheduleChore(logCleaner);

        // start the hfile archive cleaner thread
        Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
        Map<String, Object> params = new HashMap<>();
        params.put(MASTER, this);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 定时任务 HFileCleaner
         */
        this.hfileCleaner = new HFileCleaner(cleanerInterval, this, conf, getMasterFileSystem().getFileSystem(), archiveDir, cleanerPool, params);
        getChoreService().scheduleChore(hfileCleaner);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 定时任务 ReplicationBarrierCleaner
         */
        replicationBarrierCleaner = new ReplicationBarrierCleaner(conf, this, getConnection(), replicationPeerManager);
        getChoreService().scheduleChore(replicationBarrierCleaner);

        serviceStarted = true;
        if(LOG.isTraceEnabled()) {
            LOG.trace("Started service threads");
        }
    }

    @Override
    protected void stopServiceThreads() {
        if(masterJettyServer != null) {
            LOG.info("Stopping master jetty server");
            try {
                masterJettyServer.stop();
            } catch(Exception e) {
                LOG.error("Failed to stop master jetty server", e);
            }
        }
        stopChores();
        if(this.mobCompactThread != null) {
            this.mobCompactThread.close();
        }
        super.stopServiceThreads();
        if(cleanerPool != null) {
            cleanerPool.shutdownNow();
            cleanerPool = null;
        }

        LOG.debug("Stopping service threads");

        if(this.quotaManager != null) {
            this.quotaManager.stop();
        }

        if(this.activeMasterManager != null) {
            this.activeMasterManager.stop();
        }
        if(this.serverManager != null) {
            this.serverManager.stop();
        }
        if(this.assignmentManager != null) {
            this.assignmentManager.stop();
        }

        stopProcedureExecutor();

        if(this.walManager != null) {
            this.walManager.stop();
        }
        if(this.fileSystemManager != null) {
            this.fileSystemManager.stop();
        }
        if(this.mpmHost != null) {
            this.mpmHost.stop("server shutting down.");
        }
        if(this.regionServerTracker != null) {
            this.regionServerTracker.stop();
        }
    }

    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释：
     */
    private void createProcedureExecutor() throws IOException {
        MasterProcedureEnv procEnv = new MasterProcedureEnv(this);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： procedureStore 传送给了 ProcedureExecutor
         *   记住： WALProcedureStore
         */
        procedureStore = new WALProcedureStore(conf, new MasterProcedureEnv.WALStoreLeaseRecovery(this));
        procedureStore.registerListener(new ProcedureStoreListener() {

            @Override
            public void abortProcess() {
                abort("The Procedure Store lost the lease", null);
            }
        });
        MasterProcedureScheduler procedureScheduler = procEnv.getProcedureScheduler();
        procedureExecutor = new ProcedureExecutor<>(conf, procEnv, procedureStore, procedureScheduler);
        configurationManager.registerObserver(procEnv);

        int cpus = Runtime.getRuntime().availableProcessors();
        final int numThreads = conf.getInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS,
                Math.max((cpus > 0 ? cpus / 4 : 0), MasterProcedureConstants.DEFAULT_MIN_MASTER_PROCEDURE_THREADS));
        final boolean abortOnCorruption = conf
                .getBoolean(MasterProcedureConstants.EXECUTOR_ABORT_ON_CORRUPTION, MasterProcedureConstants.DEFAULT_EXECUTOR_ABORT_ON_CORRUPTION);
        procedureStore.start(numThreads);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        // Just initialize it but do not start the workers, we will start the workers later by calling startProcedureExecutor.
        // See the javadoc for finishActiveMasterInitialization for more details.
        procedureExecutor.init(numThreads, abortOnCorruption);
        procEnv.getRemoteDispatcher().start();
    }

    private void startProcedureExecutor() throws IOException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        procedureExecutor.startWorkers();
    }

    private void stopProcedureExecutor() {
        if(procedureExecutor != null) {
            configurationManager.deregisterObserver(procedureExecutor.getEnvironment());
            procedureExecutor.getEnvironment().getRemoteDispatcher().stop();
            procedureExecutor.stop();
            procedureExecutor.join();
            procedureExecutor = null;
        }

        if(procedureStore != null) {
            procedureStore.stop(isAborted());
            procedureStore = null;
        }
    }

    private void stopChores() {
        ChoreService choreService = getChoreService();
        if(choreService != null) {
            choreService.cancelChore(this.expiredMobFileCleanerChore);
            choreService.cancelChore(this.mobCompactChore);
            choreService.cancelChore(this.balancerChore);
            choreService.cancelChore(this.normalizerChore);
            choreService.cancelChore(this.clusterStatusChore);
            choreService.cancelChore(this.catalogJanitorChore);
            choreService.cancelChore(this.clusterStatusPublisherChore);
            choreService.cancelChore(this.snapshotQuotaChore);
            choreService.cancelChore(this.logCleaner);
            choreService.cancelChore(this.hfileCleaner);
            choreService.cancelChore(this.replicationBarrierCleaner);
            choreService.cancelChore(this.hbckChore);
        }
    }

    /**
     * @return Get remote side's InetAddress
     */
    InetAddress getRemoteInetAddress(final int port, final long serverStartCode) throws UnknownHostException {
        // Do it out here in its own little method so can fake an address when
        // mocking up in tests.
        InetAddress ia = RpcServer.getRemoteIp();

        // The call could be from the local regionserver,
        // in which case, there is no remote address.
        if(ia == null && serverStartCode == startcode) {
            InetSocketAddress isa = rpcServices.getSocketAddress();
            if(isa != null && isa.getPort() == port) {
                ia = isa.getAddress();
            }
        }
        return ia;
    }

    /**
     * @return Maximum time we should run balancer for
     */
    private int getMaxBalancingTime() {
        // if max balancing time isn't set, defaulting it to period time
        int maxBalancingTime = getConfiguration().getInt(HConstants.HBASE_BALANCER_MAX_BALANCING,
                getConfiguration().getInt(HConstants.HBASE_BALANCER_PERIOD, HConstants.DEFAULT_HBASE_BALANCER_PERIOD));
        return maxBalancingTime;
    }

    /**
     * @return Maximum number of regions in transition
     */
    private int getMaxRegionsInTransition() {
        int numRegions = this.assignmentManager.getRegionStates().getRegionAssignments().size();
        return Math.max((int) Math.floor(numRegions * this.maxRitPercent), 1);
    }

    /**
     * It first sleep to the next balance plan start time. Meanwhile, throttling by the max
     * number regions in transition to protect availability.
     *
     * @param nextBalanceStartTime   The next balance plan start time
     * @param maxRegionsInTransition max number of regions in transition
     * @param cutoffTime             when to exit balancer
     */
    private void balanceThrottling(long nextBalanceStartTime, int maxRegionsInTransition, long cutoffTime) {
        boolean interrupted = false;

        // Sleep to next balance plan start time
        // But if there are zero regions in transition, it can skip sleep to speed up.
        while(!interrupted && System.currentTimeMillis() < nextBalanceStartTime && this.assignmentManager.getRegionStates()
                .hasRegionsInTransition()) {
            try {
                Thread.sleep(100);
            } catch(InterruptedException ie) {
                interrupted = true;
            }
        }

        // Throttling by max number regions in transition
        while(!interrupted && maxRegionsInTransition > 0 && this.assignmentManager.getRegionStates()
                .getRegionsInTransitionCount() >= maxRegionsInTransition && System.currentTimeMillis() <= cutoffTime) {
            try {
                // sleep if the number of regions in transition exceeds the limit
                Thread.sleep(100);
            } catch(InterruptedException ie) {
                interrupted = true;
            }
        }

        if(interrupted)
            Thread.currentThread().interrupt();
    }

    public boolean balance() throws IOException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        return balance(false);
    }

    public boolean balance(boolean force) throws IOException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：如果master没有被初始化，不能运行balancer
         */
        // if master not initialized, don't run balancer.
        if(!isInitialized()) {
            LOG.debug("Master has not been initialized, don't run balancer.");
            return false;
        }

        // TODO_MA 注释：HBase 维护模式，不能进行 balance
        if(isInMaintenanceMode()) {
            LOG.info("Master is in maintenanceMode mode, don't run balancer.");
            return false;
        }

        synchronized(this.balancer) {

            // TODO_MA 注释：从 loadBalancerTracker 处获取 balancer 是否已开启，如果没有，则返回false
            // If balance not true, don't run balancer.
            if(!this.loadBalancerTracker.isBalancerOn())
                return false;

            // TODO_MA 注释：一次只能运行一个 balance 程序
            // Only allow one balance run at at time.
            if(this.assignmentManager.hasRegionsInTransition()) {
                List<RegionStateNode> regionsInTransition = assignmentManager.getRegionsInTransition();
                // if hbase:meta region is in transition, result of assignment cannot be recorded
                // ignore the force flag in that case
                boolean metaInTransition = assignmentManager.isMetaRegionInTransition();
                String prefix = force && !metaInTransition ? "R" : "Not r";
                List<RegionStateNode> toPrint = regionsInTransition;
                int max = 5;
                boolean truncated = false;
                if(regionsInTransition.size() > max) {
                    toPrint = regionsInTransition.subList(0, max);
                    truncated = true;
                }
                LOG.info(prefix + "unning balancer because " + regionsInTransition
                        .size() + " region(s) in transition: " + toPrint + (truncated ? "(truncated list)" : ""));
                if(!force || metaInTransition)
                    return false;
            }
            if(this.serverManager.areDeadServersInProgress()) {
                LOG.info("Not running balancer because processing dead regionserver(s): " + this.serverManager.getDeadServers());
                return false;
            }

            if(this.cpHost != null) {
                try {
                    if(this.cpHost.preBalance()) {
                        LOG.debug("Coprocessor bypassing balancer request");
                        return false;
                    }
                } catch(IOException ioe) {
                    LOG.error("Error invoking master coprocessor preBalance()", ioe);
                    return false;
                }
            }

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释：获取表名->{ServerName->Region列表的映射集合}的映射集合assignmentsByTable
             */
            Map<TableName, Map<ServerName, List<RegionInfo>>> assignments = this.assignmentManager.getRegionStates()
                    .getAssignmentsForBalancer(tableStateManager, this.serverManager.getOnlineServersList());

            for(Map<ServerName, List<RegionInfo>> serverMap : assignments.values()) {
                serverMap.keySet().removeAll(this.serverManager.getDrainingServersList());
            }

            //Give the balancer the current cluster state.
            this.balancer.setClusterMetrics(getClusterMetricsWithoutCoprocessor());

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释：获得Region的 移动计划列表
             */
            List<RegionPlan> plans = this.balancer.balanceCluster(assignments);

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释：循环处理这个移动计划列表plans，开始移动Region
             */
            List<RegionPlan> sucRPs = executeRegionPlansWithThrottling(plans);

            if(this.cpHost != null) {
                try {
                    this.cpHost.postBalance(sucRPs);
                } catch(IOException ioe) {
                    // balancing already succeeded so don't change the result
                    LOG.error("Error invoking master coprocessor postBalance()", ioe);
                }
            }
        }
        // If LoadBalancer did not generate any plans, it means the cluster is already balanced.
        // Return true indicating a success.
        return true;
    }

    public List<RegionPlan> executeRegionPlansWithThrottling(List<RegionPlan> plans) {
        List<RegionPlan> sucRPs = new ArrayList<>();
        int maxRegionsInTransition = getMaxRegionsInTransition();

        // TODO_MA 注释： balance 开始时间
        long balanceStartTime = System.currentTimeMillis();

        // TODO_MA 注释： balance 截止时间
        long cutoffTime = balanceStartTime + this.maxBlancingTime;

        // TODO_MA 注释： 移动的 region 的个数
        int rpCount = 0;  // number of RegionPlans balanced so far

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        if(plans != null && !plans.isEmpty()) {

            // TODO_MA 注释：平均每个 region 的移动时间
            int balanceInterval = this.maxBlancingTime / plans.size();
            LOG.info("Balancer plans size is " + plans
                    .size() + ", the balance interval is " + balanceInterval + " ms, and the max number regions in transition is " + maxRegionsInTransition);

            for(RegionPlan plan : plans) {
                LOG.info("balance " + plan);
                //TODO: bulk assign
                try {

                    /********
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *   注释：移动
                     */
                    this.assignmentManager.moveAsync(plan);
                } catch(HBaseIOException hioe) {
                    //should ignore failed plans here, avoiding the whole balance plans be aborted
                    //later calls of balance() can fetch up the failed and skipped plans
                    LOG.warn("Failed balance plan: {}, just skip it", plan, hioe);
                }

                // TODO_MA 注释：移动完成，记数+1
                //rpCount records balance plans processed, does not care if a plan succeeds
                rpCount++;

                // TODO_MA 注释：节流，目的是为了保证可用性
                if(this.maxBlancingTime > 0) {
                    balanceThrottling(balanceStartTime + rpCount * balanceInterval, maxRegionsInTransition, cutoffTime);
                }

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： balance 不能超过 cutoffTime
                 */
                // if performing next balance exceeds cutoff time, exit the loop
                if(this.maxBlancingTime > 0 && rpCount < plans.size() && System.currentTimeMillis() > cutoffTime) {
                    // TODO: After balance, there should not be a cutoff time (keeping it as
                    // a security net for now)
                    LOG.debug("No more balancing till next balance run; maxBalanceTime=" + this.maxBlancingTime);
                    break;
                }
            }
        }
        return sucRPs;
    }

    @Override
    @VisibleForTesting
    public RegionNormalizer getRegionNormalizer() {
        return this.normalizer;
    }

    /**
     * Perform normalization of cluster (invoked by {@link RegionNormalizerChore}).
     *
     * @return true if normalization step was performed successfully, false otherwise
     * (specifically, if HMaster hasn't been initialized properly or normalization
     * is globally disabled)
     */
    public boolean normalizeRegions() throws IOException {
        if(!isInitialized()) {
            LOG.debug("Master has not been initialized, don't run region normalizer.");
            return false;
        }
        if(this.getServerManager().isClusterShutdown()) {
            LOG.info("Cluster is shutting down, don't run region normalizer.");
            return false;
        }
        if(isInMaintenanceMode()) {
            LOG.info("Master is in maintenance mode, don't run region normalizer.");
            return false;
        }
        if(!this.regionNormalizerTracker.isNormalizerOn()) {
            LOG.debug("Region normalization is disabled, don't run region normalizer.");
            return false;
        }

        synchronized(this.normalizer) {
            // Don't run the normalizer concurrently
            List<TableName> allEnabledTables = new ArrayList<>(this.tableStateManager.getTablesInStates(TableState.State.ENABLED));

            Collections.shuffle(allEnabledTables);

            for(TableName table : allEnabledTables) {
                if(isInMaintenanceMode()) {
                    LOG.debug("Master is in maintenance mode, stop running region normalizer.");
                    return false;
                }

                TableDescriptor tblDesc = getTableDescriptors().get(table);
                if(table.isSystemTable() || (tblDesc != null && !tblDesc.isNormalizationEnabled())) {
                    LOG.trace("Skipping normalization for {}, as it's either system" + " table or doesn't have auto normalization turned on", table);
                    continue;
                }
                List<NormalizationPlan> plans = this.normalizer.computePlanForTable(table);
                if(plans != null) {
                    for(NormalizationPlan plan : plans) {
                        plan.execute(clusterConnection.getAdmin());
                        if(plan.getType() == PlanType.SPLIT) {
                            splitPlanCount++;
                        } else if(plan.getType() == PlanType.MERGE) {
                            mergePlanCount++;
                        }
                    }
                }
            }
        }
        // If Region did not generate any plans, it means the cluster is already balanced.
        // Return true indicating a success.
        return true;
    }

    /**
     * @return Client info for use as prefix on an audit log string; who did an action
     */
    @Override
    public String getClientIdAuditPrefix() {
        return "Client=" + RpcServer.getRequestUserName().orElse(null) + "/" + RpcServer.getRemoteAddress().orElse(null);
    }

    /**
     * Switch for the background CatalogJanitor thread.
     * Used for testing.  The thread will continue to run.  It will just be a noop
     * if disabled.
     *
     * @param b If false, the catalog janitor won't do anything.
     */
    public void setCatalogJanitorEnabled(final boolean b) {
        this.catalogJanitorChore.setEnabled(b);
    }

    @Override
    public long mergeRegions(final RegionInfo[] regionsToMerge, final boolean forcible, final long ng, final long nonce) throws IOException {
        checkInitialized();

        if(!isSplitOrMergeEnabled(MasterSwitchType.MERGE)) {
            String regionsStr = Arrays.deepToString(regionsToMerge);
            LOG.warn("Merge switch is off! skip merge of " + regionsStr);
            throw new DoNotRetryIOException("Merge of " + regionsStr + " failed because merge switch is off");
        }

        final String mergeRegionsStr = Arrays.stream(regionsToMerge).
                map(r -> RegionInfo.getShortNameToLog(r)).collect(Collectors.joining(", "));
        return MasterProcedureUtil.submitProcedure(new NonceProcedureRunnable(this, ng, nonce) {
            @Override
            protected void run() throws IOException {
                getMaster().getMasterCoprocessorHost().preMergeRegions(regionsToMerge);
                String aid = getClientIdAuditPrefix();
                LOG.info("{} merge regions {}", aid, mergeRegionsStr);
                submitProcedure(new MergeTableRegionsProcedure(procedureExecutor.getEnvironment(), regionsToMerge, forcible));
                getMaster().getMasterCoprocessorHost().postMergeRegions(regionsToMerge);
            }

            @Override
            protected String getDescription() {
                return "MergeTableProcedure";
            }
        });
    }

    @Override
    public long splitRegion(final RegionInfo regionInfo, final byte[] splitRow, final long nonceGroup, final long nonce) throws IOException {
        checkInitialized();

        if(!isSplitOrMergeEnabled(MasterSwitchType.SPLIT)) {
            LOG.warn("Split switch is off! skip split of " + regionInfo);
            throw new DoNotRetryIOException("Split region " + regionInfo.getRegionNameAsString() + " failed due to split switch off");
        }

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                getMaster().getMasterCoprocessorHost().preSplitRegion(regionInfo.getTable(), splitRow);
                LOG.info(getClientIdAuditPrefix() + " split " + regionInfo.getRegionNameAsString());

                // Execute the operation asynchronously
                submitProcedure(getAssignmentManager().createSplitProcedure(regionInfo, splitRow));
            }

            @Override
            protected String getDescription() {
                return "SplitTableProcedure";
            }
        });
    }

    // Public so can be accessed by tests. Blocks until move is done.
    // Replace with an async implementation from which you can get
    // a success/failure result.
    @VisibleForTesting
    public void move(final byte[] encodedRegionName, byte[] destServerName) throws HBaseIOException {
        RegionState regionState = assignmentManager.getRegionStates().
                getRegionState(Bytes.toString(encodedRegionName));

        RegionInfo hri;
        if(regionState != null) {
            hri = regionState.getRegion();
        } else {
            throw new UnknownRegionException(Bytes.toStringBinary(encodedRegionName));
        }

        ServerName dest;
        List<ServerName> exclude = hri.getTable().isSystemTable() ? assignmentManager.getExcludedServersForSystemTable() : new ArrayList<>(1);
        if(destServerName != null && exclude.contains(ServerName.valueOf(Bytes.toString(destServerName)))) {
            LOG.info(Bytes.toString(encodedRegionName) + " can not move to " + Bytes
                    .toString(destServerName) + " because the server is in exclude list");
            destServerName = null;
        }
        if(destServerName == null || destServerName.length == 0) {
            LOG.info("Passed destination servername is null/empty so " + "choosing a server at random");
            exclude.add(regionState.getServerName());
            final List<ServerName> destServers = this.serverManager.createDestinationServersList(exclude);
            dest = balancer.randomAssignment(hri, destServers);
            if(dest == null) {
                LOG.debug("Unable to determine a plan to assign " + hri);
                return;
            }
        } else {
            ServerName candidate = ServerName.valueOf(Bytes.toString(destServerName));
            dest = balancer.randomAssignment(hri, Lists.newArrayList(candidate));
            if(dest == null) {
                LOG.debug("Unable to determine a plan to assign " + hri);
                return;
            }
            // TODO: What is this? I don't get it.
            if(dest.equals(serverName) && balancer instanceof BaseLoadBalancer && !((BaseLoadBalancer) balancer).shouldBeOnMaster(hri)) {
                // To avoid unnecessary region moving later by balancer. Don't put user
                // regions on master.
                LOG.debug("Skipping move of region " + hri
                        .getRegionNameAsString() + " to avoid unnecessary region moving later by load balancer," + " because it should not be on master");
                return;
            }
        }

        if(dest.equals(regionState.getServerName())) {
            LOG.debug("Skipping move of region " + hri.getRegionNameAsString() + " because region already assigned to the same server " + dest + ".");
            return;
        }

        // Now we can do the move
        RegionPlan rp = new RegionPlan(hri, regionState.getServerName(), dest);
        assert rp.getDestination() != null : rp.toString() + " " + dest;

        try {
            checkInitialized();
            if(this.cpHost != null) {
                this.cpHost.preMove(hri, rp.getSource(), rp.getDestination());
            }

            TransitRegionStateProcedure proc = this.assignmentManager.createMoveRegionProcedure(rp.getRegionInfo(), rp.getDestination());
            // Warmup the region on the destination before initiating the move. this call
            // is synchronous and takes some time. doing it before the source region gets
            // closed
            serverManager.sendRegionWarmup(rp.getDestination(), hri);
            LOG.info(getClientIdAuditPrefix() + " move " + rp + ", running balancer");
            Future<byte[]> future = ProcedureSyncWait.submitProcedure(this.procedureExecutor, proc);
            try {
                // Is this going to work? Will we throw exception on error?
                // TODO: CompletableFuture rather than this stunted Future.
                future.get();
            } catch(InterruptedException | ExecutionException e) {
                throw new HBaseIOException(e);
            }
            if(this.cpHost != null) {
                this.cpHost.postMove(hri, rp.getSource(), rp.getDestination());
            }
        } catch(IOException ioe) {
            if(ioe instanceof HBaseIOException) {
                throw (HBaseIOException) ioe;
            }
            throw new HBaseIOException(ioe);
        }
    }

    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释： 创建表的请求处理
     */
    @Override
    public long createTable(final TableDescriptor tableDescriptor, final byte[][] splitKeys, final long nonceGroup,
            final long nonce) throws IOException {
        checkInitialized();

        // TODO_MA 注释：协处理器处理
        TableDescriptor desc = getMasterCoprocessorHost().preCreateTableRegionsInfos(tableDescriptor);
        if(desc == null) {
            throw new IOException("Creation for " + tableDescriptor + " is canceled by CP");
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 获取 namespace
         */
        String namespace = desc.getTableName().getNamespaceAsString();
        this.clusterSchemaService.getNamespace(namespace);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 构建 Regions
         */
        RegionInfo[] newRegions = ModifyRegionUtils.createRegionInfos(desc, splitKeys);
        TableDescriptorChecker.sanityCheck(conf, desc);

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： MasterProcedureUtil.submitProcedure 这句代码的执行，会最终回到这个 run 方法
             */
            @Override
            protected void run() throws IOException {

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 预处理
                 */
                getMaster().getMasterCoprocessorHost().preCreateTable(desc, newRegions);

                LOG.info(getClientIdAuditPrefix() + " create " + desc);

                // TODO: We can handle/merge duplicate requests, and differentiate the case of
                // TableExistsException by saying if the schema is the same or not.
                //
                // We need to wait for the procedure to potentially fail due to "prepare" sanity checks.
                // This will block only the beginning of the procedure. See HBASE-19953.
                ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 提交
                 */
                submitProcedure(new CreateTableProcedure(procedureExecutor.getEnvironment(), desc, newRegions, latch));
                latch.await();

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 提交处理
                 */
                getMaster().getMasterCoprocessorHost().postCreateTable(desc, newRegions);
            }

            @Override
            protected String getDescription() {
                return "CreateTableProcedure";
            }
        });
    }

    @Override
    public long createSystemTable(final TableDescriptor tableDescriptor) throws IOException {
        if(isStopped()) {
            throw new MasterNotRunningException();
        }

        TableName tableName = tableDescriptor.getTableName();
        if(!(tableName.isSystemTable())) {
            throw new IllegalArgumentException("Only system table creation can use this createSystemTable API");
        }

        RegionInfo[] newRegions = ModifyRegionUtils.createRegionInfos(tableDescriptor, null);

        LOG.info(getClientIdAuditPrefix() + " create " + tableDescriptor);

        // This special create table is called locally to master.  Therefore, no RPC means no need
        // to use nonce to detect duplicated RPC call.
        long procId = this.procedureExecutor
                .submitProcedure(new CreateTableProcedure(procedureExecutor.getEnvironment(), tableDescriptor, newRegions));

        return procId;
    }

    private void startActiveMasterManager(int infoPort) throws KeeperException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 一上线，就通过 MasterAddressTracker.setMasterAddress() 方法创建代表自己存在与否的 backup znode emphamaral 节点
         *   一上线，都不是 active master，所以先成为 backup master，然后创建代表自己的临时 znode 节点
         *   1、如果时机条件正确，则成为 active master，则刚才创建的 临时znode 节点被 ActiveMasterManager 显示删除
         *   2、如果不能成为 active master 不需要做什么
         *   3、如果这个 backup master宕机，则 zookeeper 会自动删除这个临时节点
         */
        String backupZNode = ZNodePaths.joinZNode(zooKeeper.getZNodePaths().backupMasterAddressesZNode, serverName.toString());
        // TODO_MA 注释：ZNodePaths.joinZNode 唯一的目的只有一个：拼接一个znode路径：
        // TODO_MA 注释： 当前master一启动就立即在 /hbsae/backup-masters/ 下面创建一个临时节点
        // TODO_MA 注释：代表自己上线了。但是并没有立即成为  active
        // TODO_MA 注释：具体的创建 znode 的方法是： MasterAddressTracker.setMasterAddress()

        /*
         * Add a ZNode for ourselves in the backup master directory since we may not become the active master.
         * If so, we want the actual active master to know we are backup masters,
         * so that it won't assign regions to us if so configured.
         *
         * If we become the active master later, ActiveMasterManager will delete this node explicitly.
         * If we crash before then, ZooKeeper will delete this node for us since it is ephemeral.
         */
        // TODO_MA 注释：这儿是去创建 Backup Master ZNode
        LOG.info("Adding backup master ZNode " + backupZNode);
        if(!MasterAddressTracker.setMasterAddress(zooKeeper, backupZNode, serverName, infoPort)) {
            LOG.warn("Failed create of " + backupZNode + " by " + serverName);
        }
        this.activeMasterManager.setInfoPort(infoPort);
        int timeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 阻塞到直到有 ActiveMaster 为止， 但是默认是 False，不会执行，除非有人显示调整这个参数为 True
         *   因为可能同时有多个 amster 启动。
         *   等待是否有 activeMaster
         */
        // If we're a backup master, stall until a primary to write this address
        if(conf.getBoolean(HConstants.MASTER_TYPE_BACKUP, HConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
            LOG.debug("HMaster started in backup mode. Stalling until master znode is written.");
            // This will only be a minute or so while the cluster starts up,
            // so don't worry about setting watches on the parent znode
            while(!activeMasterManager.hasActiveMaster()) {
                LOG.debug("Waiting for master address and cluster state znode to be written.");
                Threads.sleep(timeout);
            }
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 开始启动 日志提示
         */
        MonitoredTask status = TaskMonitor.get().createStatus("Master startup");
        status.setDescription("Master startup");

        try {

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： if 中的方法，完成注册和争抢成为 Active Master 的工作
             */
            if(activeMasterManager.blockUntilBecomingActiveMaster(timeout, status)) {

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 这个方法，只有 Active Master 才运行
                 *   active amster 跟 backup master 在启动的时候会不一样的
                 */
                finishActiveMasterInitialization(status);
            }
        } catch(Throwable t) {
            status.setStatus("Failed to become active: " + t.getMessage());
            LOG.error(HBaseMarkers.FATAL, "Failed to become active master", t);
            // HBASE-5680: Likely hadoop23 vs hadoop 20.x/1.x incompatibility
            if(t instanceof NoClassDefFoundError && t.getMessage().
                    contains("org/apache/hadoop/hdfs/protocol/HdfsConstants$SafeModeAction")) {
                // improved error message for this special case
                abort("HBase is having a problem with its Hadoop jars.  You may need to recompile " + "HBase against Hadoop version " + org.apache.hadoop.util.VersionInfo
                        .getVersion() + " or change your hadoop jars to start properly", t);
            } else {
                abort("Unhandled exception. Starting shutdown.", t);
            }
        } finally {
            status.cleanup();
        }
    }

    private static boolean isCatalogTable(final TableName tableName) {
        return tableName.equals(TableName.META_TABLE_NAME);
    }

    @Override
    public long deleteTable(final TableName tableName, final long nonceGroup, final long nonce) throws IOException {
        checkInitialized();

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                getMaster().getMasterCoprocessorHost().preDeleteTable(tableName);

                LOG.info(getClientIdAuditPrefix() + " delete " + tableName);

                // TODO: We can handle/merge duplicate request
                //
                // We need to wait for the procedure to potentially fail due to "prepare" sanity
                // checks. This will block only the beginning of the procedure. See HBASE-19953.
                ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
                submitProcedure(new DeleteTableProcedure(procedureExecutor.getEnvironment(), tableName, latch));
                latch.await();

                getMaster().getMasterCoprocessorHost().postDeleteTable(tableName);
            }

            @Override
            protected String getDescription() {
                return "DeleteTableProcedure";
            }
        });
    }

    @Override
    public long truncateTable(final TableName tableName, final boolean preserveSplits, final long nonceGroup, final long nonce) throws IOException {
        checkInitialized();

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                getMaster().getMasterCoprocessorHost().preTruncateTable(tableName);

                LOG.info(getClientIdAuditPrefix() + " truncate " + tableName);
                ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch(2, 0);
                submitProcedure(new TruncateTableProcedure(procedureExecutor.getEnvironment(), tableName, preserveSplits, latch));
                latch.await();

                getMaster().getMasterCoprocessorHost().postTruncateTable(tableName);
            }

            @Override
            protected String getDescription() {
                return "TruncateTableProcedure";
            }
        });
    }

    @Override
    public long addColumn(final TableName tableName, final ColumnFamilyDescriptor column, final long nonceGroup,
            final long nonce) throws IOException {
        checkInitialized();
        checkTableExists(tableName);

        return modifyTable(tableName, new TableDescriptorGetter() {

            @Override
            public TableDescriptor get() throws IOException {
                TableDescriptor old = getTableDescriptors().get(tableName);
                if(old.hasColumnFamily(column.getName())) {
                    throw new InvalidFamilyOperationException(
                            "Column family '" + column.getNameAsString() + "' in table '" + tableName + "' already exists so cannot be added");
                }

                return TableDescriptorBuilder.newBuilder(old).setColumnFamily(column).build();
            }
        }, nonceGroup, nonce, true);
    }

    /**
     * Implement to return TableDescriptor after pre-checks
     */
    protected interface TableDescriptorGetter {
        TableDescriptor get() throws IOException;
    }

    @Override
    public long modifyColumn(final TableName tableName, final ColumnFamilyDescriptor descriptor, final long nonceGroup,
            final long nonce) throws IOException {
        checkInitialized();
        checkTableExists(tableName);
        return modifyTable(tableName, new TableDescriptorGetter() {

            @Override
            public TableDescriptor get() throws IOException {
                TableDescriptor old = getTableDescriptors().get(tableName);
                if(!old.hasColumnFamily(descriptor.getName())) {
                    throw new InvalidFamilyOperationException(
                            "Family '" + descriptor.getNameAsString() + "' does not exist, so it cannot be modified");
                }

                return TableDescriptorBuilder.newBuilder(old).modifyColumnFamily(descriptor).build();
            }
        }, nonceGroup, nonce, true);
    }

    @Override
    public long deleteColumn(final TableName tableName, final byte[] columnName, final long nonceGroup, final long nonce) throws IOException {
        checkInitialized();
        checkTableExists(tableName);

        return modifyTable(tableName, new TableDescriptorGetter() {

            @Override
            public TableDescriptor get() throws IOException {
                TableDescriptor old = getTableDescriptors().get(tableName);

                if(!old.hasColumnFamily(columnName)) {
                    throw new InvalidFamilyOperationException("Family '" + Bytes.toString(columnName) + "' does not exist, so it cannot be deleted");
                }
                if(old.getColumnFamilyCount() == 1) {
                    throw new InvalidFamilyOperationException(
                            "Family '" + Bytes.toString(columnName) + "' is the only column family in the table, so it cannot be deleted");
                }
                return TableDescriptorBuilder.newBuilder(old).removeColumnFamily(columnName).build();
            }
        }, nonceGroup, nonce, true);
    }

    @Override
    public long enableTable(final TableName tableName, final long nonceGroup, final long nonce) throws IOException {
        checkInitialized();

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                getMaster().getMasterCoprocessorHost().preEnableTable(tableName);

                // Normally, it would make sense for this authorization check to exist inside
                // AccessController, but because the authorization check is done based on internal state
                // (rather than explicit permissions) we'll do the check here instead of in the
                // coprocessor.
                MasterQuotaManager quotaManager = getMasterQuotaManager();
                if(quotaManager != null) {
                    if(quotaManager.isQuotaInitialized()) {
                        SpaceQuotaSnapshot currSnapshotOfTable = QuotaTableUtil.getCurrentSnapshotFromQuotaTable(getConnection(), tableName);
                        if(currSnapshotOfTable != null) {
                            SpaceQuotaStatus quotaStatus = currSnapshotOfTable.getQuotaStatus();
                            if(quotaStatus.isInViolation() && SpaceViolationPolicy.DISABLE == quotaStatus.getPolicy().orElse(null)) {
                                throw new AccessDeniedException(
                                        "Enabling the table '" + tableName + "' is disallowed due to a violated space quota.");
                            }
                        }
                    } else if(LOG.isTraceEnabled()) {
                        LOG.trace("Unable to check for space quotas as the MasterQuotaManager is not enabled");
                    }
                }

                LOG.info(getClientIdAuditPrefix() + " enable " + tableName);

                // Execute the operation asynchronously - client will check the progress of the operation
                // In case the request is from a <1.1 client before returning,
                // we want to make sure that the table is prepared to be
                // enabled (the table is locked and the table state is set).
                // Note: if the procedure throws exception, we will catch it and rethrow.
                final ProcedurePrepareLatch prepareLatch = ProcedurePrepareLatch.createLatch();
                submitProcedure(new EnableTableProcedure(procedureExecutor.getEnvironment(), tableName, prepareLatch));
                prepareLatch.await();

                getMaster().getMasterCoprocessorHost().postEnableTable(tableName);
            }

            @Override
            protected String getDescription() {
                return "EnableTableProcedure";
            }
        });
    }

    @Override
    public long disableTable(final TableName tableName, final long nonceGroup, final long nonce) throws IOException {
        checkInitialized();

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                getMaster().getMasterCoprocessorHost().preDisableTable(tableName);

                LOG.info(getClientIdAuditPrefix() + " disable " + tableName);

                // Execute the operation asynchronously - client will check the progress of the operation
                // In case the request is from a <1.1 client before returning,
                // we want to make sure that the table is prepared to be
                // enabled (the table is locked and the table state is set).
                // Note: if the procedure throws exception, we will catch it and rethrow.
                //
                // We need to wait for the procedure to potentially fail due to "prepare" sanity
                // checks. This will block only the beginning of the procedure. See HBASE-19953.
                final ProcedurePrepareLatch prepareLatch = ProcedurePrepareLatch.createBlockingLatch();
                submitProcedure(new DisableTableProcedure(procedureExecutor.getEnvironment(), tableName, false, prepareLatch));
                prepareLatch.await();

                getMaster().getMasterCoprocessorHost().postDisableTable(tableName);
            }

            @Override
            protected String getDescription() {
                return "DisableTableProcedure";
            }
        });
    }

    private long modifyTable(final TableName tableName, final TableDescriptorGetter newDescriptorGetter, final long nonceGroup, final long nonce,
            final boolean shouldCheckDescriptor) throws IOException {
        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                TableDescriptor oldDescriptor = getMaster().getTableDescriptors().get(tableName);
                TableDescriptor newDescriptor = getMaster().getMasterCoprocessorHost()
                        .preModifyTable(tableName, oldDescriptor, newDescriptorGetter.get());
                TableDescriptorChecker.sanityCheck(conf, newDescriptor);
                LOG.info("{} modify table {} from {} to {}", getClientIdAuditPrefix(), tableName, oldDescriptor, newDescriptor);

                // Execute the operation synchronously - wait for the operation completes before
                // continuing.
                //
                // We need to wait for the procedure to potentially fail due to "prepare" sanity
                // checks. This will block only the beginning of the procedure. See HBASE-19953.
                ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
                submitProcedure(
                        new ModifyTableProcedure(procedureExecutor.getEnvironment(), newDescriptor, latch, oldDescriptor, shouldCheckDescriptor));
                latch.await();

                getMaster().getMasterCoprocessorHost().postModifyTable(tableName, oldDescriptor, newDescriptor);
            }

            @Override
            protected String getDescription() {
                return "ModifyTableProcedure";
            }
        });

    }

    @Override
    public long modifyTable(final TableName tableName, final TableDescriptor newDescriptor, final long nonceGroup,
            final long nonce) throws IOException {
        checkInitialized();
        return modifyTable(tableName, new TableDescriptorGetter() {
            @Override
            public TableDescriptor get() throws IOException {
                return newDescriptor;
            }
        }, nonceGroup, nonce, false);

    }

    public long restoreSnapshot(final SnapshotDescription snapshotDesc, final long nonceGroup, final long nonce,
            final boolean restoreAcl) throws IOException {
        checkInitialized();
        getSnapshotManager().checkSnapshotSupport();

        // Ensure namespace exists. Will throw exception if non-known NS.
        final TableName dstTable = TableName.valueOf(snapshotDesc.getTable());
        getClusterSchema().getNamespace(dstTable.getNamespaceAsString());

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                setProcId(getSnapshotManager().restoreOrCloneSnapshot(snapshotDesc, getNonceKey(), restoreAcl));
            }

            @Override
            protected String getDescription() {
                return "RestoreSnapshotProcedure";
            }
        });
    }

    private void checkTableExists(final TableName tableName) throws IOException, TableNotFoundException {
        if(!MetaTableAccessor.tableExists(getConnection(), tableName)) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public void checkTableModifiable(final TableName tableName) throws IOException, TableNotFoundException, TableNotDisabledException {
        if(isCatalogTable(tableName)) {
            throw new IOException("Can't modify catalog tables");
        }
        checkTableExists(tableName);
        TableState ts = getTableStateManager().getTableState(tableName);
        if(!ts.isDisabled()) {
            throw new TableNotDisabledException("Not DISABLED; " + ts);
        }
    }

    public ClusterMetrics getClusterMetricsWithoutCoprocessor() throws InterruptedIOException {
        return getClusterMetricsWithoutCoprocessor(EnumSet.allOf(Option.class));
    }

    public ClusterMetrics getClusterMetricsWithoutCoprocessor(EnumSet<Option> options) throws InterruptedIOException {
        ClusterMetricsBuilder builder = ClusterMetricsBuilder.newBuilder();
        // given that hbase1 can't submit the request with Option,
        // we return all information to client if the list of Option is empty.
        if(options.isEmpty()) {
            options = EnumSet.allOf(Option.class);
        }

        for(Option opt : options) {
            switch(opt) {
                case HBASE_VERSION:
                    builder.setHBaseVersion(VersionInfo.getVersion());
                    break;
                case CLUSTER_ID:
                    builder.setClusterId(getClusterId());
                    break;
                case MASTER:
                    builder.setMasterName(getServerName());
                    break;
                case BACKUP_MASTERS:
                    builder.setBackerMasterNames(getBackupMasters());
                    break;
                case LIVE_SERVERS: {
                    if(serverManager != null) {
                        builder.setLiveServerMetrics(
                                serverManager.getOnlineServers().entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
                    }
                    break;
                }
                case DEAD_SERVERS: {
                    if(serverManager != null) {
                        builder.setDeadServerNames(new ArrayList<>(serverManager.getDeadServers().copyServerNames()));
                    }
                    break;
                }
                case MASTER_COPROCESSORS: {
                    if(cpHost != null) {
                        builder.setMasterCoprocessorNames(Arrays.asList(getMasterCoprocessors()));
                    }
                    break;
                }
                case REGIONS_IN_TRANSITION: {
                    if(assignmentManager != null) {
                        builder.setRegionsInTransition(assignmentManager.getRegionStates().getRegionsStateInTransition());
                    }
                    break;
                }
                case BALANCER_ON: {
                    if(loadBalancerTracker != null) {
                        builder.setBalancerOn(loadBalancerTracker.isBalancerOn());
                    }
                    break;
                }
                case MASTER_INFO_PORT: {
                    if(infoServer != null) {
                        builder.setMasterInfoPort(infoServer.getPort());
                    }
                    break;
                }
                case SERVERS_NAME: {
                    if(serverManager != null) {
                        builder.setServerNames(serverManager.getOnlineServersList());
                    }
                    break;
                }
                case TABLE_TO_REGIONS_COUNT: {
                    if(isActiveMaster() && isInitialized() && assignmentManager != null) {
                        try {
                            Map<TableName, RegionStatesCount> tableRegionStatesCountMap = new HashMap<>();
                            Map<String, TableDescriptor> tableDescriptorMap = getTableDescriptors().getAll();
                            for(TableDescriptor tableDescriptor : tableDescriptorMap.values()) {
                                TableName tableName = tableDescriptor.getTableName();
                                RegionStatesCount regionStatesCount = assignmentManager.getRegionStatesCount(tableName);
                                tableRegionStatesCountMap.put(tableName, regionStatesCount);
                            }
                            builder.setTableRegionStatesCount(tableRegionStatesCountMap);
                        } catch(IOException e) {
                            LOG.error("Error while populating TABLE_TO_REGIONS_COUNT for Cluster Metrics..", e);
                        }
                    }
                    break;
                }
            }
        }
        return builder.build();
    }

    /**
     * @return cluster status
     */
    public ClusterMetrics getClusterMetrics() throws IOException {
        return getClusterMetrics(EnumSet.allOf(Option.class));
    }

    public ClusterMetrics getClusterMetrics(EnumSet<Option> options) throws IOException {
        if(cpHost != null) {
            cpHost.preGetClusterMetrics();
        }
        ClusterMetrics status = getClusterMetricsWithoutCoprocessor(options);
        if(cpHost != null) {
            cpHost.postGetClusterMetrics(status);
        }
        return status;
    }

    private List<ServerName> getBackupMasters() throws InterruptedIOException {
        // Build Set of backup masters from ZK nodes
        List<String> backupMasterStrings;
        try {
            backupMasterStrings = ZKUtil.listChildrenNoWatch(this.zooKeeper, this.zooKeeper.getZNodePaths().backupMasterAddressesZNode);
        } catch(KeeperException e) {
            LOG.warn(this.zooKeeper.prefix("Unable to list backup servers"), e);
            backupMasterStrings = null;
        }

        List<ServerName> backupMasters = Collections.emptyList();
        if(backupMasterStrings != null && !backupMasterStrings.isEmpty()) {
            backupMasters = new ArrayList<>(backupMasterStrings.size());
            for(String s : backupMasterStrings) {
                try {
                    byte[] bytes;
                    try {
                        bytes = ZKUtil.getData(this.zooKeeper, ZNodePaths.joinZNode(this.zooKeeper.getZNodePaths().backupMasterAddressesZNode, s));
                    } catch(InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                    if(bytes != null) {
                        ServerName sn;
                        try {
                            sn = ProtobufUtil.parseServerNameFrom(bytes);
                        } catch(DeserializationException e) {
                            LOG.warn("Failed parse, skipping registering backup server", e);
                            continue;
                        }
                        backupMasters.add(sn);
                    }
                } catch(KeeperException e) {
                    LOG.warn(this.zooKeeper.prefix("Unable to get information about " + "backup servers"), e);
                }
            }
            Collections.sort(backupMasters, new Comparator<ServerName>() {
                @Override
                public int compare(ServerName s1, ServerName s2) {
                    return s1.getServerName().compareTo(s2.getServerName());
                }
            });
        }
        return backupMasters;
    }

    /**
     * The set of loaded coprocessors is stored in a static set. Since it's
     * statically allocated, it does not require that HMaster's cpHost be
     * initialized prior to accessing it.
     *
     * @return a String representation of the set of names of the loaded coprocessors.
     */
    public static String getLoadedCoprocessors() {
        return CoprocessorHost.getLoadedCoprocessors().toString();
    }

    /**
     * @return timestamp in millis when HMaster was started.
     */
    public long getMasterStartTime() {
        return startcode;
    }

    /**
     * @return timestamp in millis when HMaster became the active master.
     */
    public long getMasterActiveTime() {
        return masterActiveTime;
    }

    /**
     * @return timestamp in millis when HMaster finished becoming the active master
     */
    public long getMasterFinishedInitializationTime() {
        return masterFinishedInitializationTime;
    }

    public int getNumWALFiles() {
        return procedureStore != null ? procedureStore.getActiveLogs().size() : 0;
    }

    public WALProcedureStore getWalProcedureStore() {
        return procedureStore;
    }

    public int getRegionServerInfoPort(final ServerName sn) {
        int port = this.serverManager.getInfoPort(sn);
        return port == 0 ? conf.getInt(HConstants.REGIONSERVER_INFO_PORT, HConstants.DEFAULT_REGIONSERVER_INFOPORT) : port;
    }

    @Override
    public String getRegionServerVersion(ServerName sn) {
        // Will return "0.0.0" if the server is not online to prevent move system region to unknown
        // version RS.
        return this.serverManager.getVersion(sn);
    }

    @Override
    public void checkIfShouldMoveSystemRegionAsync() {
        assignmentManager.checkIfShouldMoveSystemRegionAsync();
    }

    /**
     * @return array of coprocessor SimpleNames.
     */
    public String[] getMasterCoprocessors() {
        Set<String> masterCoprocessors = getMasterCoprocessorHost().getCoprocessors();
        return masterCoprocessors.toArray(new String[masterCoprocessors.size()]);
    }

    @Override
    public void abort(String reason, Throwable cause) {
        if(isAborted() || isStopped()) {
            return;
        }
        setAbortRequested();
        if(cpHost != null) {
            // HBASE-4014: dump a list of loaded coprocessors.
            LOG.error(HBaseMarkers.FATAL, "Master server abort: loaded coprocessors are: " + getLoadedCoprocessors());
        }
        String msg = "***** ABORTING master " + this + ": " + reason + " *****";
        if(cause != null) {
            LOG.error(HBaseMarkers.FATAL, msg, cause);
        } else {
            LOG.error(HBaseMarkers.FATAL, msg);
        }

        try {
            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 停止
             */
            stopMaster();
        } catch(IOException e) {
            LOG.error("Exception occurred while stopping master", e);
        }
    }

    @Override
    public ZKWatcher getZooKeeper() {
        return zooKeeper;
    }

    @Override
    public MasterCoprocessorHost getMasterCoprocessorHost() {
        return cpHost;
    }

    @Override
    public MasterQuotaManager getMasterQuotaManager() {
        return quotaManager;
    }

    @Override
    public ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
        return procedureExecutor;
    }

    @Override
    public ServerName getServerName() {
        return this.serverName;
    }

    @Override
    public AssignmentManager getAssignmentManager() {
        return this.assignmentManager;
    }

    @Override
    public CatalogJanitor getCatalogJanitor() {
        return this.catalogJanitorChore;
    }

    public MemoryBoundedLogMessageBuffer getRegionServerFatalLogBuffer() {
        return rsFatals;
    }

    /**
     * Shutdown the cluster.
     * Master runs a coordinated stop of all RegionServers and then itself.
     */
    public void shutdown() throws IOException {
        if(cpHost != null) {
            cpHost.preShutdown();
        }

        // Tell the servermanager cluster shutdown has been called. This makes it so when Master is
        // last running server, it'll stop itself. Next, we broadcast the cluster shutdown by setting
        // the cluster status as down. RegionServers will notice this change in state and will start
        // shutting themselves down. When last has exited, Master can go down.
        if(this.serverManager != null) {
            this.serverManager.shutdownCluster();
        }
        if(this.clusterStatusTracker != null) {
            try {
                this.clusterStatusTracker.setClusterDown();
            } catch(KeeperException e) {
                LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
            }
        }
        // Stop the procedure executor. Will stop any ongoing assign, unassign, server crash etc.,
        // processing so we can go down.
        if(this.procedureExecutor != null) {
            this.procedureExecutor.stop();
        }
        // Shutdown our cluster connection. This will kill any hosted RPCs that might be going on;
        // this is what we want especially if the Master is in startup phase doing call outs to
        // hbase:meta, etc. when cluster is down. Without ths connection close, we'd have to wait on
        // the rpc to timeout.
        if(this.clusterConnection != null) {
            this.clusterConnection.close();
        }
    }

    public void stopMaster() throws IOException {
        if(cpHost != null) {
            cpHost.preStopMaster();
        }
        stop("Stopped by " + Thread.currentThread().getName());
    }

    @Override
    public void stop(String msg) {
        if(!isStopped()) {
            super.stop(msg);
            if(this.activeMasterManager != null) {
                this.activeMasterManager.stop();
            }
        }
    }

    @VisibleForTesting
    protected void checkServiceStarted() throws ServerNotRunningYetException {
        if(!serviceStarted) {
            throw new ServerNotRunningYetException("Server is not running yet");
        }
    }

    public static class MasterStoppedException extends DoNotRetryIOException {
        MasterStoppedException() {
            super();
        }
    }

    void checkInitialized() throws PleaseHoldException, ServerNotRunningYetException, MasterNotRunningException, MasterStoppedException {
        checkServiceStarted();
        if(!isInitialized()) {
            throw new PleaseHoldException("Master is initializing");
        }
        if(isStopped()) {
            throw new MasterStoppedException();
        }
    }

    /**
     * Report whether this master is currently the active master or not.
     * If not active master, we are parked on ZK waiting to become active.
     *
     * This method is used for testing.
     *
     * @return true if active master, false if not.
     */
    @Override
    public boolean isActiveMaster() {
        return activeMaster;
    }

    /**
     * Report whether this master has completed with its initialization and is
     * ready.  If ready, the master is also the active master.  A standby master
     * is never ready.
     *
     * This method is used for testing.
     *
     * @return true if master is ready to go, false if not.
     */
    @Override
    public boolean isInitialized() {
        return initialized.isReady();
    }

    /**
     * Report whether this master is in maintenance mode.
     *
     * @return true if master is in maintenanceMode
     */
    @Override
    public boolean isInMaintenanceMode() {
        return maintenanceMode;
    }

    @VisibleForTesting
    public void setInitialized(boolean isInitialized) {
        procedureExecutor.getEnvironment().setEventReady(initialized, isInitialized);
    }

    @Override
    public ProcedureEvent<?> getInitializedEvent() {
        return initialized;
    }

    /**
     * Compute the average load across all region servers.
     * Currently, this uses a very naive computation - just uses the number of
     * regions being served, ignoring stats about number of requests.
     *
     * @return the average load
     */
    public double getAverageLoad() {
        if(this.assignmentManager == null) {
            return 0;
        }

        RegionStates regionStates = this.assignmentManager.getRegionStates();
        if(regionStates == null) {
            return 0;
        }
        return regionStates.getAverageLoad();
    }

    /*
     * @return the count of region split plans executed
     */
    public long getSplitPlanCount() {
        return splitPlanCount;
    }

    /*
     * @return the count of region merge plans executed
     */
    public long getMergePlanCount() {
        return mergePlanCount;
    }

    @Override
    public boolean registerService(Service instance) {
        /*
         * No stacking of instances is allowed for a single service name
         */
        Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
        String serviceName = CoprocessorRpcUtils.getServiceName(serviceDesc);
        if(coprocessorServiceHandlers.containsKey(serviceName)) {
            LOG.error("Coprocessor service " + serviceName + " already registered, rejecting request from " + instance);
            return false;
        }

        coprocessorServiceHandlers.put(serviceName, instance);
        if(LOG.isDebugEnabled()) {
            LOG.debug("Registered master coprocessor service: service=" + serviceName);
        }
        return true;
    }

    /**
     * Utility for constructing an instance of the passed HMaster class.
     *
     * @param masterClass
     * @return HMaster instance.
     */
    public static HMaster constructMaster(Class<? extends HMaster> masterClass, final Configuration conf) {
        try {

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 获取 HMaster 对象实例
             */
            Constructor<? extends HMaster> c = masterClass.getConstructor(Configuration.class);

            // TODO_MA 注释： HMaster 只有一个构造器 当然也就只有一个 Configuration 的参数
            return c.newInstance(conf);

        } catch(Exception e) {
            Throwable error = e;
            if(e instanceof InvocationTargetException && ((InvocationTargetException) e).getTargetException() != null) {
                error = ((InvocationTargetException) e).getTargetException();
            }
            throw new RuntimeException("Failed construction of Master: " + masterClass.toString() + ". ", error);
        }
    }

    /**
     * @see org.apache.hadoop.hbase.master.HMasterCommandLine
     */
    public static void main(String[] args) {

        // TODO_MA 注释： 输出版本号
        LOG.info("STARTING service " + HMaster.class.getSimpleName());
        VersionInfo.logVersion();

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 跳转到 HMasterCommandLine 的 run() 方法
         *   HMaster.class = masterClass 成员变量
         */
        new HMasterCommandLine(HMaster.class).doMain(args);
    }

    public HFileCleaner getHFileCleaner() {
        return this.hfileCleaner;
    }

    public LogCleaner getLogCleaner() {
        return this.logCleaner;
    }

    /**
     * @return the underlying snapshot manager
     */
    @Override
    public SnapshotManager getSnapshotManager() {
        return this.snapshotManager;
    }

    /**
     * @return the underlying MasterProcedureManagerHost
     */
    @Override
    public MasterProcedureManagerHost getMasterProcedureManagerHost() {
        return mpmHost;
    }

    @Override
    public ClusterSchema getClusterSchema() {
        return this.clusterSchemaService;
    }

    /**
     * Create a new Namespace.
     *
     * @param namespaceDescriptor descriptor for new Namespace
     * @param nonceGroup          Identifier for the source of the request, a client or process.
     * @param nonce               A unique identifier for this operation from the client or process identified by
     *                            <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
     * @return procedure id
     */
    long createNamespace(final NamespaceDescriptor namespaceDescriptor, final long nonceGroup, final long nonce) throws IOException {
        checkInitialized();

        TableName.isLegalNamespaceName(Bytes.toBytes(namespaceDescriptor.getName()));

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                getMaster().getMasterCoprocessorHost().preCreateNamespace(namespaceDescriptor);
                // We need to wait for the procedure to potentially fail due to "prepare" sanity
                // checks. This will block only the beginning of the procedure. See HBASE-19953.
                ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
                LOG.info(getClientIdAuditPrefix() + " creating " + namespaceDescriptor);
                // Execute the operation synchronously - wait for the operation to complete before
                // continuing.
                setProcId(getClusterSchema().createNamespace(namespaceDescriptor, getNonceKey(), latch));
                latch.await();
                getMaster().getMasterCoprocessorHost().postCreateNamespace(namespaceDescriptor);
            }

            @Override
            protected String getDescription() {
                return "CreateNamespaceProcedure";
            }
        });
    }

    /**
     * Modify an existing Namespace.
     *
     * @param nonceGroup Identifier for the source of the request, a client or process.
     * @param nonce      A unique identifier for this operation from the client or process identified by
     *                   <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
     * @return procedure id
     */
    long modifyNamespace(final NamespaceDescriptor newNsDescriptor, final long nonceGroup, final long nonce) throws IOException {
        checkInitialized();

        TableName.isLegalNamespaceName(Bytes.toBytes(newNsDescriptor.getName()));

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                NamespaceDescriptor oldNsDescriptor = getNamespace(newNsDescriptor.getName());
                getMaster().getMasterCoprocessorHost().preModifyNamespace(oldNsDescriptor, newNsDescriptor);
                // We need to wait for the procedure to potentially fail due to "prepare" sanity
                // checks. This will block only the beginning of the procedure. See HBASE-19953.
                ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
                LOG.info(getClientIdAuditPrefix() + " modify " + newNsDescriptor);
                // Execute the operation synchronously - wait for the operation to complete before
                // continuing.
                setProcId(getClusterSchema().modifyNamespace(newNsDescriptor, getNonceKey(), latch));
                latch.await();
                getMaster().getMasterCoprocessorHost().postModifyNamespace(oldNsDescriptor, newNsDescriptor);
            }

            @Override
            protected String getDescription() {
                return "ModifyNamespaceProcedure";
            }
        });
    }

    /**
     * Delete an existing Namespace. Only empty Namespaces (no tables) can be removed.
     *
     * @param nonceGroup Identifier for the source of the request, a client or process.
     * @param nonce      A unique identifier for this operation from the client or process identified by
     *                   <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
     * @return procedure id
     */
    long deleteNamespace(final String name, final long nonceGroup, final long nonce) throws IOException {
        checkInitialized();

        return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
            @Override
            protected void run() throws IOException {
                getMaster().getMasterCoprocessorHost().preDeleteNamespace(name);
                LOG.info(getClientIdAuditPrefix() + " delete " + name);
                // Execute the operation synchronously - wait for the operation to complete before
                // continuing.
                //
                // We need to wait for the procedure to potentially fail due to "prepare" sanity
                // checks. This will block only the beginning of the procedure. See HBASE-19953.
                ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
                setProcId(submitProcedure(new DeleteNamespaceProcedure(procedureExecutor.getEnvironment(), name, latch)));
                latch.await();
                // Will not be invoked in the face of Exception thrown by the Procedure's execution
                getMaster().getMasterCoprocessorHost().postDeleteNamespace(name);
            }

            @Override
            protected String getDescription() {
                return "DeleteNamespaceProcedure";
            }
        });
    }

    /**
     * Get a Namespace
     *
     * @param name Name of the Namespace
     * @return Namespace descriptor for <code>name</code>
     */
    NamespaceDescriptor getNamespace(String name) throws IOException {
        checkInitialized();
        if(this.cpHost != null)
            this.cpHost.preGetNamespaceDescriptor(name);
        NamespaceDescriptor nsd = this.clusterSchemaService.getNamespace(name);
        if(this.cpHost != null)
            this.cpHost.postGetNamespaceDescriptor(nsd);
        return nsd;
    }

    /**
     * Get all Namespaces
     *
     * @return All Namespace descriptors
     */
    List<NamespaceDescriptor> getNamespaces() throws IOException {
        checkInitialized();
        final List<NamespaceDescriptor> nsds = new ArrayList<>();
        if(cpHost != null) {
            cpHost.preListNamespaceDescriptors(nsds);
        }
        nsds.addAll(this.clusterSchemaService.getNamespaces());
        if(this.cpHost != null) {
            this.cpHost.postListNamespaceDescriptors(nsds);
        }
        return nsds;
    }

    @Override
    public List<TableName> listTableNamesByNamespace(String name) throws IOException {
        checkInitialized();
        return listTableNames(name, null, true);
    }

    @Override
    public List<TableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException {
        checkInitialized();
        return listTableDescriptors(name, null, null, true);
    }

    @Override
    public boolean abortProcedure(final long procId, final boolean mayInterruptIfRunning) throws IOException {
        if(cpHost != null) {
            cpHost.preAbortProcedure(this.procedureExecutor, procId);
        }

        final boolean result = this.procedureExecutor.abort(procId, mayInterruptIfRunning);

        if(cpHost != null) {
            cpHost.postAbortProcedure();
        }

        return result;
    }

    @Override
    public List<Procedure<?>> getProcedures() throws IOException {
        if(cpHost != null) {
            cpHost.preGetProcedures();
        }

        @SuppressWarnings({"unchecked", "rawtypes"}) List<Procedure<?>> procList = (List) this.procedureExecutor.getProcedures();

        if(cpHost != null) {
            cpHost.postGetProcedures(procList);
        }

        return procList;
    }

    @Override
    public List<LockedResource> getLocks() throws IOException {
        if(cpHost != null) {
            cpHost.preGetLocks();
        }

        MasterProcedureScheduler procedureScheduler = procedureExecutor.getEnvironment().getProcedureScheduler();

        final List<LockedResource> lockedResources = procedureScheduler.getLocks();

        if(cpHost != null) {
            cpHost.postGetLocks(lockedResources);
        }

        return lockedResources;
    }

    /**
     * Returns the list of table descriptors that match the specified request
     *
     * @param namespace        the namespace to query, or null if querying for all
     * @param regex            The regular expression to match against, or null if querying for all
     * @param tableNameList    the list of table names, or null if querying for all
     * @param includeSysTables False to match only against userspace tables
     * @return the list of table descriptors
     */
    public List<TableDescriptor> listTableDescriptors(final String namespace, final String regex, final List<TableName> tableNameList,
            final boolean includeSysTables) throws IOException {
        List<TableDescriptor> htds = new ArrayList<>();
        if(cpHost != null) {
            cpHost.preGetTableDescriptors(tableNameList, htds, regex);
        }
        htds = getTableDescriptors(htds, namespace, regex, tableNameList, includeSysTables);
        if(cpHost != null) {
            cpHost.postGetTableDescriptors(tableNameList, htds, regex);
        }
        return htds;
    }

    /**
     * Returns the list of table names that match the specified request
     *
     * @param regex            The regular expression to match against, or null if querying for all
     * @param namespace        the namespace to query, or null if querying for all
     * @param includeSysTables False to match only against userspace tables
     * @return the list of table names
     */
    public List<TableName> listTableNames(final String namespace, final String regex, final boolean includeSysTables) throws IOException {
        List<TableDescriptor> htds = new ArrayList<>();
        if(cpHost != null) {
            cpHost.preGetTableNames(htds, regex);
        }
        htds = getTableDescriptors(htds, namespace, regex, null, includeSysTables);
        if(cpHost != null) {
            cpHost.postGetTableNames(htds, regex);
        }
        List<TableName> result = new ArrayList<>(htds.size());
        for(TableDescriptor htd : htds)
            result.add(htd.getTableName());
        return result;
    }

    /**
     * @return list of table table descriptors after filtering by regex and whether to include system
     * tables, etc.
     * @throws IOException
     */
    private List<TableDescriptor> getTableDescriptors(final List<TableDescriptor> htds, final String namespace, final String regex,
            final List<TableName> tableNameList, final boolean includeSysTables) throws IOException {
        if(tableNameList == null || tableNameList.isEmpty()) {
            // request for all TableDescriptors
            Collection<TableDescriptor> allHtds;
            if(namespace != null && namespace.length() > 0) {
                // Do a check on the namespace existence. Will fail if does not exist.
                this.clusterSchemaService.getNamespace(namespace);
                allHtds = tableDescriptors.getByNamespace(namespace).values();
            } else {
                allHtds = tableDescriptors.getAll().values();
            }
            for(TableDescriptor desc : allHtds) {
                if(tableStateManager.isTablePresent(desc.getTableName()) && (includeSysTables || !desc.getTableName().isSystemTable())) {
                    htds.add(desc);
                }
            }
        } else {
            for(TableName s : tableNameList) {
                if(tableStateManager.isTablePresent(s)) {
                    TableDescriptor desc = tableDescriptors.get(s);
                    if(desc != null) {
                        htds.add(desc);
                    }
                }
            }
        }

        // Retains only those matched by regular expression.
        if(regex != null)
            filterTablesByRegex(htds, Pattern.compile(regex));
        return htds;
    }

    /**
     * Removes the table descriptors that don't match the pattern.
     *
     * @param descriptors list of table descriptors to filter
     * @param pattern     the regex to use
     */
    private static void filterTablesByRegex(final Collection<TableDescriptor> descriptors, final Pattern pattern) {
        final String defaultNS = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
        Iterator<TableDescriptor> itr = descriptors.iterator();
        while(itr.hasNext()) {
            TableDescriptor htd = itr.next();
            String tableName = htd.getTableName().getNameAsString();
            boolean matched = pattern.matcher(tableName).matches();
            if(!matched && htd.getTableName().getNamespaceAsString().equals(defaultNS)) {
                matched = pattern.matcher(defaultNS + TableName.NAMESPACE_DELIM + tableName).matches();
            }
            if(!matched) {
                itr.remove();
            }
        }
    }

    @Override
    public long getLastMajorCompactionTimestamp(TableName table) throws IOException {
        return getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLastMajorCompactionTimestamp(table);
    }

    @Override
    public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
        return getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLastMajorCompactionTimestamp(regionName);
    }

    /**
     * Gets the mob file compaction state for a specific table.
     * Whether all the mob files are selected is known during the compaction execution, but
     * the statistic is done just before compaction starts, it is hard to know the compaction
     * type at that time, so the rough statistics are chosen for the mob file compaction. Only two
     * compaction states are available, CompactionState.MAJOR_AND_MINOR and CompactionState.NONE.
     *
     * @param tableName The current table name.
     * @return If a given table is in mob file compaction now.
     */
    public CompactionState getMobCompactionState(TableName tableName) {
        AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
        if(compactionsCount != null && compactionsCount.get() != 0) {
            return CompactionState.MAJOR_AND_MINOR;
        }
        return CompactionState.NONE;
    }

    public void reportMobCompactionStart(TableName tableName) throws IOException {
        IdLock.Entry lockEntry = null;
        try {
            lockEntry = mobCompactionLock.getLockEntry(tableName.hashCode());
            AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
            if(compactionsCount == null) {
                compactionsCount = new AtomicInteger(0);
                mobCompactionStates.put(tableName, compactionsCount);
            }
            compactionsCount.incrementAndGet();
        } finally {
            if(lockEntry != null) {
                mobCompactionLock.releaseLockEntry(lockEntry);
            }
        }
    }

    public void reportMobCompactionEnd(TableName tableName) throws IOException {
        IdLock.Entry lockEntry = null;
        try {
            lockEntry = mobCompactionLock.getLockEntry(tableName.hashCode());
            AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
            if(compactionsCount != null) {
                int count = compactionsCount.decrementAndGet();
                // remove the entry if the count is 0.
                if(count == 0) {
                    mobCompactionStates.remove(tableName);
                }
            }
        } finally {
            if(lockEntry != null) {
                mobCompactionLock.releaseLockEntry(lockEntry);
            }
        }
    }

    /**
     * Requests mob compaction.
     *
     * @param tableName The table the compact.
     * @param columns   The compacted columns.
     * @param allFiles  Whether add all mob files into the compaction.
     */
    public void requestMobCompaction(TableName tableName, List<ColumnFamilyDescriptor> columns, boolean allFiles) throws IOException {
        mobCompactThread.requestMobCompaction(conf, fs, tableName, columns, allFiles);
    }

    /**
     * Queries the state of the {@link LoadBalancerTracker}. If the balancer is not initialized,
     * false is returned.
     *
     * @return The state of the load balancer, or false if the load balancer isn't defined.
     */
    public boolean isBalancerOn() {
        return !isInMaintenanceMode() && loadBalancerTracker != null && loadBalancerTracker.isBalancerOn();
    }

    /**
     * Queries the state of the {@link RegionNormalizerTracker}. If it's not initialized,
     * false is returned.
     */
    public boolean isNormalizerOn() {
        return !isInMaintenanceMode() && regionNormalizerTracker != null && regionNormalizerTracker.isNormalizerOn();
    }

    /**
     * Queries the state of the {@link SplitOrMergeTracker}. If it is not initialized,
     * false is returned. If switchType is illegal, false will return.
     *
     * @param switchType see {@link org.apache.hadoop.hbase.client.MasterSwitchType}
     * @return The state of the switch
     */
    @Override
    public boolean isSplitOrMergeEnabled(MasterSwitchType switchType) {
        return !isInMaintenanceMode() && splitOrMergeTracker != null && splitOrMergeTracker.isSplitOrMergeEnabled(switchType);
    }

    /**
     * Fetch the configured {@link LoadBalancer} class name. If none is set, a default is returned.
     *
     * @return The name of the {@link LoadBalancer} in use.
     */
    public String getLoadBalancerClassName() {
        return conf.get(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, LoadBalancerFactory.getDefaultLoadBalancerClass().getName());
    }

    /**
     * @return RegionNormalizerTracker instance
     */
    public RegionNormalizerTracker getRegionNormalizerTracker() {
        return regionNormalizerTracker;
    }

    public SplitOrMergeTracker getSplitOrMergeTracker() {
        return splitOrMergeTracker;
    }

    @Override
    public LoadBalancer getLoadBalancer() {
        return balancer;
    }

    @Override
    public FavoredNodesManager getFavoredNodesManager() {
        return favoredNodesManager;
    }

    private long executePeerProcedure(ModifyPeerProcedure procedure) throws IOException {
        long procId = procedureExecutor.submitProcedure(procedure);
        procedure.getLatch().await();
        return procId;
    }

    @Override
    public long addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled) throws ReplicationException, IOException {
        LOG.info(
                getClientIdAuditPrefix() + " creating replication peer, id=" + peerId + ", config=" + peerConfig + ", state=" + (enabled ? "ENABLED" : "DISABLED"));
        return executePeerProcedure(new AddPeerProcedure(peerId, peerConfig, enabled));
    }

    @Override
    public long removeReplicationPeer(String peerId) throws ReplicationException, IOException {
        LOG.info(getClientIdAuditPrefix() + " removing replication peer, id=" + peerId);
        return executePeerProcedure(new RemovePeerProcedure(peerId));
    }

    @Override
    public long enableReplicationPeer(String peerId) throws ReplicationException, IOException {
        LOG.info(getClientIdAuditPrefix() + " enable replication peer, id=" + peerId);
        return executePeerProcedure(new EnablePeerProcedure(peerId));
    }

    @Override
    public long disableReplicationPeer(String peerId) throws ReplicationException, IOException {
        LOG.info(getClientIdAuditPrefix() + " disable replication peer, id=" + peerId);
        return executePeerProcedure(new DisablePeerProcedure(peerId));
    }

    @Override
    public ReplicationPeerConfig getReplicationPeerConfig(String peerId) throws ReplicationException, IOException {
        if(cpHost != null) {
            cpHost.preGetReplicationPeerConfig(peerId);
        }
        LOG.info(getClientIdAuditPrefix() + " get replication peer config, id=" + peerId);
        ReplicationPeerConfig peerConfig = this.replicationPeerManager.getPeerConfig(peerId)
                .orElseThrow(() -> new ReplicationPeerNotFoundException(peerId));
        if(cpHost != null) {
            cpHost.postGetReplicationPeerConfig(peerId);
        }
        return peerConfig;
    }

    @Override
    public long updateReplicationPeerConfig(String peerId, ReplicationPeerConfig peerConfig) throws ReplicationException, IOException {
        LOG.info(getClientIdAuditPrefix() + " update replication peer config, id=" + peerId + ", config=" + peerConfig);
        return executePeerProcedure(new UpdatePeerConfigProcedure(peerId, peerConfig));
    }

    @Override
    public List<ReplicationPeerDescription> listReplicationPeers(String regex) throws ReplicationException, IOException {
        if(cpHost != null) {
            cpHost.preListReplicationPeers(regex);
        }
        LOG.debug("{} list replication peers, regex={}", getClientIdAuditPrefix(), regex);
        Pattern pattern = regex == null ? null : Pattern.compile(regex);
        List<ReplicationPeerDescription> peers = this.replicationPeerManager.listPeers(pattern);
        if(cpHost != null) {
            cpHost.postListReplicationPeers(regex);
        }
        return peers;
    }

    /**
     * Mark region server(s) as decommissioned (previously called 'draining') to prevent additional
     * regions from getting assigned to them. Also unload the regions on the servers asynchronously.0
     *
     * @param servers Region servers to decommission.
     */
    public void decommissionRegionServers(final List<ServerName> servers, final boolean offload) throws HBaseIOException {
        List<ServerName> serversAdded = new ArrayList<>(servers.size());
        // Place the decommission marker first.
        String parentZnode = getZooKeeper().getZNodePaths().drainingZNode;
        for(ServerName server : servers) {
            try {
                String node = ZNodePaths.joinZNode(parentZnode, server.getServerName());
                ZKUtil.createAndFailSilent(getZooKeeper(), node);
            } catch(KeeperException ke) {
                throw new HBaseIOException(this.zooKeeper.prefix("Unable to decommission '" + server.getServerName() + "'."), ke);
            }
            if(this.serverManager.addServerToDrainList(server)) {
                serversAdded.add(server);
            }
            ;
        }
        // Move the regions off the decommissioned servers.
        if(offload) {
            final List<ServerName> destServers = this.serverManager.createDestinationServersList();
            for(ServerName server : serversAdded) {
                final List<RegionInfo> regionsOnServer = this.assignmentManager.getRegionsOnServer(server);
                for(RegionInfo hri : regionsOnServer) {
                    ServerName dest = balancer.randomAssignment(hri, destServers);
                    if(dest == null) {
                        throw new HBaseIOException("Unable to determine a plan to move " + hri);
                    }
                    RegionPlan rp = new RegionPlan(hri, server, dest);
                    this.assignmentManager.moveAsync(rp);
                }
            }
        }
    }

    /**
     * List region servers marked as decommissioned (previously called 'draining') to not get regions
     * assigned to them.
     *
     * @return List of decommissioned servers.
     */
    public List<ServerName> listDecommissionedRegionServers() {
        return this.serverManager.getDrainingServersList();
    }

    /**
     * Remove decommission marker (previously called 'draining') from a region server to allow regions
     * assignments. Load regions onto the server asynchronously if a list of regions is given
     *
     * @param server Region server to remove decommission marker from.
     */
    public void recommissionRegionServer(final ServerName server, final List<byte[]> encodedRegionNames) throws IOException {
        // Remove the server from decommissioned (draining) server list.
        String parentZnode = getZooKeeper().getZNodePaths().drainingZNode;
        String node = ZNodePaths.joinZNode(parentZnode, server.getServerName());
        try {
            ZKUtil.deleteNodeFailSilent(getZooKeeper(), node);
        } catch(KeeperException ke) {
            throw new HBaseIOException(this.zooKeeper.prefix("Unable to recommission '" + server.getServerName() + "'."), ke);
        }
        this.serverManager.removeServerFromDrainList(server);

        // Load the regions onto the server if we are given a list of regions.
        if(encodedRegionNames == null || encodedRegionNames.isEmpty()) {
            return;
        }
        if(!this.serverManager.isServerOnline(server)) {
            return;
        }
        for(byte[] encodedRegionName : encodedRegionNames) {
            RegionState regionState = assignmentManager.getRegionStates().getRegionState(Bytes.toString(encodedRegionName));
            if(regionState == null) {
                LOG.warn("Unknown region " + Bytes.toStringBinary(encodedRegionName));
                continue;
            }
            RegionInfo hri = regionState.getRegion();
            if(server.equals(regionState.getServerName())) {
                LOG.info("Skipping move of region " + hri
                        .getRegionNameAsString() + " because region already assigned to the same server " + server + ".");
                continue;
            }
            RegionPlan rp = new RegionPlan(hri, regionState.getServerName(), server);
            this.assignmentManager.moveAsync(rp);
        }
    }

    @Override
    public LockManager getLockManager() {
        return lockManager;
    }

    public QuotaObserverChore getQuotaObserverChore() {
        return this.quotaObserverChore;
    }

    public SpaceQuotaSnapshotNotifier getSpaceQuotaSnapshotNotifier() {
        return this.spaceQuotaSnapshotNotifier;
    }

    @SuppressWarnings("unchecked")
    private RemoteProcedure<MasterProcedureEnv, ?> getRemoteProcedure(long procId) {
        Procedure<?> procedure = procedureExecutor.getProcedure(procId);
        if(procedure == null) {
            return null;
        }
        assert procedure instanceof RemoteProcedure;
        return (RemoteProcedure<MasterProcedureEnv, ?>) procedure;
    }

    public void remoteProcedureCompleted(long procId) {
        LOG.debug("Remote procedure done, pid={}", procId);
        RemoteProcedure<MasterProcedureEnv, ?> procedure = getRemoteProcedure(procId);
        if(procedure != null) {
            procedure.remoteOperationCompleted(procedureExecutor.getEnvironment());
        }
    }

    public void remoteProcedureFailed(long procId, RemoteProcedureException error) {
        LOG.debug("Remote procedure failed, pid={}", procId, error);
        RemoteProcedure<MasterProcedureEnv, ?> procedure = getRemoteProcedure(procId);
        if(procedure != null) {
            procedure.remoteOperationFailed(procedureExecutor.getEnvironment(), error);
        }
    }

    @Override
    public ReplicationPeerManager getReplicationPeerManager() {
        return replicationPeerManager;
    }

    public HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>> getReplicationLoad(ServerName[] serverNames) {
        List<ReplicationPeerDescription> peerList = this.getReplicationPeerManager().listPeers(null);
        if(peerList == null) {
            return null;
        }
        HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>> replicationLoadSourceMap = new HashMap<>(peerList.size());
        peerList.stream().forEach(peer -> replicationLoadSourceMap.put(peer.getPeerId(), new ArrayList<>()));
        for(ServerName serverName : serverNames) {
            List<ReplicationLoadSource> replicationLoadSources = getServerManager().getLoad(serverName).getReplicationLoadSourceList();
            for(ReplicationLoadSource replicationLoadSource : replicationLoadSources) {
                replicationLoadSourceMap.get(replicationLoadSource.getPeerID()).add(new Pair<>(serverName, replicationLoadSource));
            }
        }
        for(List<Pair<ServerName, ReplicationLoadSource>> loads : replicationLoadSourceMap.values()) {
            if(loads.size() > 0) {
                loads.sort(Comparator.comparingLong(load -> (-1) * load.getSecond().getReplicationLag()));
            }
        }
        return replicationLoadSourceMap;
    }

    /**
     * This method modifies the master's configuration in order to inject replication-related features
     */
    @VisibleForTesting
    public static void decorateMasterConfiguration(Configuration conf) {
        String plugins = conf.get(HBASE_MASTER_LOGCLEANER_PLUGINS);
        String cleanerClass = ReplicationLogCleaner.class.getCanonicalName();
        if(!plugins.contains(cleanerClass)) {
            conf.set(HBASE_MASTER_LOGCLEANER_PLUGINS, plugins + "," + cleanerClass);
        }
        if(ReplicationUtils.isReplicationForBulkLoadDataEnabled(conf)) {
            plugins = conf.get(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
            cleanerClass = ReplicationHFileCleaner.class.getCanonicalName();
            if(!plugins.contains(cleanerClass)) {
                conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, plugins + "," + cleanerClass);
            }
        }
    }

    @Override
    public Map<String, ReplicationStatus> getWalGroupsReplicationStatus() {
        if(!this.isOnline() || !LoadBalancer.isMasterCanHostUserRegions(conf)) {
            return new HashMap<>();
        }
        return super.getWalGroupsReplicationStatus();
    }

    public HbckChore getHbckChore() {
        return this.hbckChore;
    }

    @Override
    public void runReplicationBarrierCleaner() {
        ReplicationBarrierCleaner rbc = this.replicationBarrierCleaner;
        if(rbc != null) {
            rbc.chore();
        }
    }

    public SnapshotQuotaObserverChore getSnapshotQuotaObserverChore() {
        return this.snapshotQuotaChore;
    }
}
