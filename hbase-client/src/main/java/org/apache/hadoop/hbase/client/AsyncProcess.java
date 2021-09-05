/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.AsyncProcessTask.SubmittedRows;
import org.apache.hadoop.hbase.client.RequestController.ReturnCode;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 此类允许连续的请求流。它被编写为与 HTable等同步调用程序兼容。
 * This class  allows a continuous flow of requests. It's written to be compatible with a synchronous caller such as HTable.
 *
 * 调用者通过调用提交来发送操作缓冲区。
 * 此类从此列表中提取它可以发送的操作，即在不被视为繁忙的区域中的操作。
 * 该过程是异步的，即，如果列表中的迭代完成，它将立即返回。
 * 当且仅当达到当前任务的最大数量时，提交呼叫才会阻塞。
 * 或者，调用者可以调用SubmitAll，在这种情况下，将发送所有操作。
 * 每个提交请求都返回一个类似 Future 的对象，该对象可用于跟踪操作进度。
 * The caller sends a buffer of operation, by calling submit. This class extract from this list
 * the operations it can send, i.e. the operations that are on region that are not considered as busy.
 * The process is asynchronous, i.e. it returns immediately when if has finished to iterate on the list.
 * If, and only if, the maximum number of current task is reached, the call to submit will block.
 * Alternatively, the caller can call submitAll, in which case all the operations will be sent.
 * Each call to submit returns a future-like object that can be used to track operation progress.
 *
 * 该类在内部管理重试。
 * The class manages internally the retries.
 *
 * 在返回的Future对象中跟踪错误。
 * 结果始终在Future对象内部跟踪，并且可以在调用完成后检索。
 * 如果多请求的某些部分失败，也可以检索部分结果。
 * The errors are tracked inside the Future object that is returned.
 * The results are always tracked inside the Future object and can be retrieved when the call
 * has finished. Partial results can also be retrieved if some part of multi-request failed.
 *
 * 此类是线程安全的。 在内部，该类具有足够的线程安全性，可以同时管理新的提交和旧操作产生的结果。
 * This class is thread safe.
 * Internally, the class is thread safe enough to manage simultaneously new submission and results arising from older operations.
 *
 * 在内部，此类与{@link Row}一起使用，这意味着从理论上讲，它也可以用于获取。
 * Internally, this class works with {@link Row}, this mean it could be theoretically used for gets as well.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AsyncProcess {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncProcess.class);
    private static final AtomicLong COUNTER = new AtomicLong();

    public static final String PRIMARY_CALL_TIMEOUT_KEY = "hbase.client.primaryCallTimeout.multiget";

    /**
     * Configure the number of failures after which the client will start logging. A few failures
     * is fine: region moved, then is not opened, then is overloaded. We try to have an acceptable
     * heuristic for the number of errors we don't log. 5 was chosen because we wait for 1s at
     * this stage.
     */
    public static final String START_LOG_ERRORS_AFTER_COUNT_KEY = "hbase.client.start.log.errors.counter";
    public static final int DEFAULT_START_LOG_ERRORS_AFTER_COUNT = 5;

    /**
     * Configuration to decide whether to log details for batch error
     */
    public static final String LOG_DETAILS_FOR_BATCH_ERROR = "hbase.client.log.batcherrors.details";

    /**
     * Return value from a submit that didn't contain any requests.
     */
    private static final AsyncRequestFuture NO_REQS_RESULT = new AsyncRequestFuture() {
        final Object[] result = new Object[0];

        @Override
        public boolean hasError() {
            return false;
        }

        @Override
        public RetriesExhaustedWithDetailsException getErrors() {
            return null;
        }

        @Override
        public List<? extends Row> getFailedOperations() {
            return null;
        }

        @Override
        public Object[] getResults() {
            return result;
        }

        @Override
        public void waitUntilDone() throws InterruptedIOException {
        }
    };

    // TODO: many of the fields should be made private
    final long id;

    final ClusterConnection connection;
    private final RpcRetryingCallerFactory rpcCallerFactory;
    final RpcControllerFactory rpcFactory;

    // Start configuration settings.
    final int startLogErrorsCnt;

    final long pause;
    final long pauseForCQTBE;// pause for CallQueueTooBigException, if specified
    final int numTries;
    @VisibleForTesting
    long serverTrackerTimeout;
    final long primaryCallTimeoutMicroseconds;
    /**
     * Whether to log details for batch errors
     */
    final boolean logBatchErrorDetails;
    // End configuration settings.

    /**
     * The traffic control for requests.
     */
    @VisibleForTesting
    final RequestController requestController;
    public static final String LOG_DETAILS_PERIOD = "hbase.client.log.detail.period.ms";
    private static final int DEFAULT_LOG_DETAILS_PERIOD = 10000;
    private final int periodToLog;

    AsyncProcess(ClusterConnection hc, Configuration conf, RpcRetryingCallerFactory rpcCaller, RpcControllerFactory rpcFactory) {
        if(hc == null) {
            throw new IllegalArgumentException("ClusterConnection cannot be null.");
        }

        this.connection = hc;

        this.id = COUNTER.incrementAndGet();

        this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
        long configuredPauseForCQTBE = conf.getLong(HConstants.HBASE_CLIENT_PAUSE_FOR_CQTBE, pause);
        if(configuredPauseForCQTBE < pause) {
            LOG.warn(
                    "The " + HConstants.HBASE_CLIENT_PAUSE_FOR_CQTBE + " setting: " + configuredPauseForCQTBE + " is smaller than " + HConstants.HBASE_CLIENT_PAUSE + ", will use " + pause + " instead.");
            this.pauseForCQTBE = pause;
        } else {
            this.pauseForCQTBE = configuredPauseForCQTBE;
        }
        // how many times we could try in total, one more than retry number
        this.numTries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER) + 1;
        this.primaryCallTimeoutMicroseconds = conf.getInt(PRIMARY_CALL_TIMEOUT_KEY, 10000);
        this.startLogErrorsCnt = conf.getInt(START_LOG_ERRORS_AFTER_COUNT_KEY, DEFAULT_START_LOG_ERRORS_AFTER_COUNT);
        this.periodToLog = conf.getInt(LOG_DETAILS_PERIOD, DEFAULT_LOG_DETAILS_PERIOD);
        // Server tracker allows us to do faster, and yet useful (hopefully), retries.
        // However, if we are too useful, we might fail very quickly due to retry count limit.
        // To avoid this, we are going to cheat for now (see HBASE-7659), and calculate maximum
        // retry time if normal retries were used. Then we will retry until this time runs out.
        // If we keep hitting one server, the net effect will be the incremental backoff, and
        // essentially the same number of retries as planned. If we have to do faster retries,
        // we will do more retries in aggregate, but the user will be none the wiser.
        this.serverTrackerTimeout = 0L;
        for(int i = 0; i < this.numTries; ++i) {
            serverTrackerTimeout = serverTrackerTimeout + ConnectionUtils.getPauseTime(this.pause, i);
        }

        this.rpcCallerFactory = rpcCaller;
        this.rpcFactory = rpcFactory;
        this.logBatchErrorDetails = conf.getBoolean(LOG_DETAILS_FOR_BATCH_ERROR, false);

        this.requestController = RequestControllerFactory.create(conf);
    }

    /**
     * The submitted task may be not accomplished at all if there are too many running tasks or
     * other limits.
     *
     * @param <CResult> The class to cast the result
     * @param task      The setting and data
     * @return AsyncRequestFuture
     */
    public <CResult> AsyncRequestFuture submit(AsyncProcessTask<CResult> task) throws InterruptedIOException {
        AsyncRequestFuture reqFuture = checkTask(task);
        if(reqFuture != null) {
            return reqFuture;
        }
        SubmittedRows submittedRows = task.getSubmittedRows() == null ? SubmittedRows.ALL : task.getSubmittedRows();
        switch(submittedRows) {
            case ALL:

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释：
                 */
                return submitAll(task);
            case AT_LEAST_ONE:
                return submit(task, true);
            default:
                return submit(task, false);
        }
    }

    /**
     * Extract from the rows list what we can submit. The rows we can not submit are kept in the
     * list. Does not send requests to replicas (not currently used for anything other
     * than streaming puts anyway).
     *
     * @param task       The setting and data
     * @param atLeastOne true if we should submit at least a subset.
     */
    private <CResult> AsyncRequestFuture submit(AsyncProcessTask<CResult> task, boolean atLeastOne) throws InterruptedIOException {
        TableName tableName = task.getTableName();
        RowAccess<? extends Row> rows = task.getRowAccess();
        Map<ServerName, MultiAction> actionsByServer = new HashMap<>();
        List<Action> retainedActions = new ArrayList<>(rows.size());

        NonceGenerator ng = this.connection.getNonceGenerator();
        long nonceGroup = ng.getNonceGroup(); // Currently, nonce group is per entire client.

        // Location errors that happen before we decide what requests to take.
        List<Exception> locationErrors = null;
        List<Integer> locationErrorRows = null;
        RequestController.Checker checker = requestController.newChecker();
        boolean firstIter = true;
        do {
            // Wait until there is at least one slot for a new task.
            requestController.waitForFreeSlot(id, periodToLog, getLogger(tableName, -1));
            int posInList = -1;
            if(!firstIter) {
                checker.reset();
            }
            Iterator<? extends Row> it = rows.iterator();
            while(it.hasNext()) {
                Row r = it.next();
                HRegionLocation loc;
                try {
                    if(r == null) {
                        throw new IllegalArgumentException("#" + id + ", row cannot be null");
                    }
                    // Make sure we get 0-s replica.
                    RegionLocations locs = connection.locateRegion(tableName, r.getRow(), true, true, RegionReplicaUtil.DEFAULT_REPLICA_ID);
                    if(locs == null || locs.isEmpty() || locs.getDefaultRegionLocation() == null) {
                        throw new IOException("#" + id + ", no location found, aborting submit for" + " tableName=" + tableName + " rowkey=" + Bytes
                                .toStringBinary(r.getRow()));
                    }
                    loc = locs.getDefaultRegionLocation();
                } catch(IOException ex) {
                    locationErrors = new ArrayList<>(1);
                    locationErrorRows = new ArrayList<>(1);
                    LOG.error("Failed to get region location ", ex);
                    // This action failed before creating ars. Retain it, but do not add to submit list.
                    // We will then add it to ars in an already-failed state.

                    int priority = HConstants.NORMAL_QOS;
                    if(r instanceof Mutation) {
                        priority = ((Mutation) r).getPriority();
                    }
                    retainedActions.add(new Action(r, ++posInList, priority));
                    locationErrors.add(ex);
                    locationErrorRows.add(posInList);
                    it.remove();
                    break; // Backward compat: we stop considering actions on location error.
                }
                ReturnCode code = checker.canTakeRow(loc, r);
                if(code == ReturnCode.END) {
                    break;
                }
                if(code == ReturnCode.INCLUDE) {
                    int priority = HConstants.NORMAL_QOS;
                    if(r instanceof Mutation) {
                        priority = ((Mutation) r).getPriority();
                    }
                    Action action = new Action(r, ++posInList, priority);
                    setNonce(ng, r, action);
                    retainedActions.add(action);
                    // TODO: replica-get is not supported on this path
                    byte[] regionName = loc.getRegionInfo().getRegionName();
                    addAction(loc.getServerName(), regionName, action, actionsByServer, nonceGroup);
                    it.remove();
                }
            }
            firstIter = false;
        } while(retainedActions.isEmpty() && atLeastOne && (locationErrors == null));

        if(retainedActions.isEmpty())
            return NO_REQS_RESULT;

        return submitMultiActions(task, retainedActions, nonceGroup, locationErrors, locationErrorRows, actionsByServer);
    }

    <CResult> AsyncRequestFuture submitMultiActions(AsyncProcessTask task, List<Action> retainedActions, long nonceGroup,
            List<Exception> locationErrors, List<Integer> locationErrorRows, Map<ServerName, MultiAction> actionsByServer) {
        AsyncRequestFutureImpl<CResult> ars = createAsyncRequestFuture(task, retainedActions, nonceGroup);
        // Add location errors if any
        if(locationErrors != null) {
            for(int i = 0; i < locationErrors.size(); ++i) {
                int originalIndex = locationErrorRows.get(i);
                Row row = retainedActions.get(originalIndex).getAction();
                ars.manageError(originalIndex, row, AsyncRequestFutureImpl.Retry.NO_LOCATION_PROBLEM, locationErrors.get(i), null);
            }
        }
        ars.sendMultiAction(actionsByServer, 1, null, false);
        return ars;
    }

    /**
     * Helper that is used when grouping the actions per region server.
     *
     * @param server          - server
     * @param regionName      - regionName
     * @param action          - the action to add to the multiaction
     * @param actionsByServer the multiaction per server
     * @param nonceGroup      Nonce group.
     */
    static void addAction(ServerName server, byte[] regionName, Action action, Map<ServerName, MultiAction> actionsByServer, long nonceGroup) {
        MultiAction multiAction = actionsByServer.get(server);
        if(multiAction == null) {
            multiAction = new MultiAction();
            actionsByServer.put(server, multiAction);
        }
        if(action.hasNonce() && !multiAction.hasNonceGroup()) {
            multiAction.setNonceGroup(nonceGroup);
        }

        multiAction.add(regionName, action);
    }

    /**
     * Submit immediately the list of rows, whatever the server status. Kept for backward
     * compatibility: it allows to be used with the batch interface that return an array of objects.
     *
     * @param task The setting and data
     */
    private <CResult> AsyncRequestFuture submitAll(AsyncProcessTask task) {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 把 row 列表转换为 Action 列表
         */
        RowAccess<? extends Row> rows = task.getRowAccess();
        List<Action> actions = new ArrayList<>(rows.size());

        // The position will be used by the processBatch to match the object array returned.
        int posInList = -1;
        NonceGenerator ng = this.connection.getNonceGenerator();
        int highestPriority = HConstants.PRIORITY_UNSET;
        for(Row r : rows) {
            posInList++;
            if(r instanceof Put) {
                Put put = (Put) r;
                if(put.isEmpty()) {
                    throw new IllegalArgumentException("No columns to insert for #" + (posInList + 1) + " item");
                }
                highestPriority = Math.max(put.getPriority(), highestPriority);
            }
            Action action = new Action(r, posInList, highestPriority);
            setNonce(ng, r, action);
            actions.add(action);
        }

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        AsyncRequestFutureImpl<CResult> ars = createAsyncRequestFuture(task, actions, ng.getNonceGroup());
        ars.groupAndSendMultiAction(actions, 1);
        return ars;
    }

    private <CResult> AsyncRequestFuture checkTask(AsyncProcessTask<CResult> task) {
        if(task.getRowAccess() == null || task.getRowAccess().isEmpty()) {
            return NO_REQS_RESULT;
        }
        Objects.requireNonNull(task.getPool(), "The pool can't be NULL");
        checkOperationTimeout(task.getOperationTimeout());
        checkRpcTimeout(task.getRpcTimeout());
        return null;
    }

    private void setNonce(NonceGenerator ng, Row r, Action action) {
        if(!(r instanceof Append) && !(r instanceof Increment))
            return;
        action.setNonce(ng.newNonce()); // Action handles NO_NONCE, so it's ok if ng is disabled.
    }

    private int checkTimeout(String name, int timeout) {
        if(timeout < 0) {
            throw new RuntimeException("The " + name + " must be bigger than zero," + "current value is" + timeout);
        }
        return timeout;
    }

    private int checkOperationTimeout(int operationTimeout) {
        return checkTimeout("operation timeout", operationTimeout);
    }

    private int checkRpcTimeout(int rpcTimeout) {
        return checkTimeout("rpc timeout", rpcTimeout);
    }

    @VisibleForTesting
    <CResult> AsyncRequestFutureImpl<CResult> createAsyncRequestFuture(AsyncProcessTask task, List<Action> actions, long nonceGroup) {
        return new AsyncRequestFutureImpl<>(task, actions, nonceGroup, this);
    }

    /**
     * Wait until the async does not have more than max tasks in progress.
     */
    protected void waitForMaximumCurrentTasks(int max, TableName tableName) throws InterruptedIOException {
        requestController.waitForMaximumCurrentTasks(max, id, periodToLog, getLogger(tableName, max));
    }

    private Consumer<Long> getLogger(TableName tableName, long max) {
        return (currentInProgress) -> {
            LOG.info(
                    "#" + id + (max < 0 ? ", waiting for any free slot" : ", waiting for some tasks to finish. Expected max=" + max) + ", tasksInProgress=" + currentInProgress + (tableName == null ? "" : ", tableName=" + tableName));
        };
    }

    void incTaskCounters(Collection<byte[]> regions, ServerName sn) {
        requestController.incTaskCounters(regions, sn);
    }


    void decTaskCounters(Collection<byte[]> regions, ServerName sn) {
        requestController.decTaskCounters(regions, sn);
    }

    /**
     * Create a caller. Isolated to be easily overridden in the tests.
     */
    @VisibleForTesting
    protected RpcRetryingCaller<AbstractResponse> createCaller(CancellableRegionServerCallable callable, int rpcTimeout) {
        return rpcCallerFactory.<AbstractResponse>newCaller(checkRpcTimeout(rpcTimeout));
    }


    /**
     * Creates the server error tracker to use inside process.
     * Currently, to preserve the main assumption about current retries, and to work well with
     * the retry-limit-based calculation, the calculation is local per Process object.
     * We may benefit from connection-wide tracking of server errors.
     *
     * @return ServerErrorTracker to use, null if there is no ServerErrorTracker on this connection
     */
    ConnectionImplementation.ServerErrorTracker createServerErrorTracker() {
        return new ConnectionImplementation.ServerErrorTracker(this.serverTrackerTimeout, this.numTries);
    }

    static boolean isReplicaGet(Row row) {
        return (row instanceof Get) && (((Get) row).getConsistency() == Consistency.TIMELINE);
    }

}
