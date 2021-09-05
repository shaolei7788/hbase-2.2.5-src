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
package org.apache.hadoop.hbase.ipc;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

import java.net.SocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * Factory to create a {@link org.apache.hadoop.hbase.ipc.RpcClient}
 */
@InterfaceAudience.Private
public final class RpcClientFactory {

    public static final String CUSTOM_RPC_CLIENT_IMPL_CONF_KEY = "hbase.rpc.client.impl";

    private static final ImmutableMap<String, String> DEPRECATED_NAME_MAPPING = ImmutableMap
            .of(
                    "org.apache.hadoop.hbase.ipc.RpcClientImpl", BlockingRpcClient.class.getName(),
                    "org.apache.hadoop.hbase.ipc.AsyncRpcClient", NettyRpcClient.class.getName()
            );

    /**
     * Private Constructor
     */
    private RpcClientFactory() {
    }

    /**
     * Helper method for tests only. Creates an {@code RpcClient} without metrics.
     */
    @VisibleForTesting
    public static RpcClient createClient(Configuration conf, String clusterId) {
        return createClient(conf, clusterId, null);
    }

    /**
     * Creates a new RpcClient by the class defined in the configuration or falls back to
     * RpcClientImpl
     *
     * @param conf      configuration
     * @param clusterId the cluster id
     * @param metrics   the connection metrics
     * @return newly created RpcClient
     */
    public static RpcClient createClient(Configuration conf, String clusterId, MetricsConnection metrics) {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： NettyRpcClient
         */
        return createClient(conf, clusterId, null, metrics);
    }

    /********
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *   注释： NettyRpcClient
     */
    private static String getRpcClientClass(Configuration conf) {
        String rpcClientClass = conf.get(CUSTOM_RPC_CLIENT_IMPL_CONF_KEY);
        if(rpcClientClass == null) {

            // TODO_MA 注释：默认实现
            return NettyRpcClient.class.getName();
        }
        String mappedName = DEPRECATED_NAME_MAPPING.get(rpcClientClass);
        return mappedName == null ? rpcClientClass : mappedName;
    }

    /**
     * Creates a new RpcClient by the class defined in the configuration or falls back to
     * RpcClientImpl
     *
     * @param conf      configuration
     * @param clusterId the cluster id
     * @param localAddr client socket bind address.
     * @param metrics   the connection metrics
     * @return newly created RpcClient
     */
    public static RpcClient createClient(Configuration conf, String clusterId, SocketAddress localAddr, MetricsConnection metrics) {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 获取 RPCClient 类：NettyRpcClient
         */
        String rpcClientClass = getRpcClientClass(conf);

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 通过反射进行实例化
         */
        return ReflectionUtils.instantiateWithCustomCtor(rpcClientClass,
                new Class[]{Configuration.class, String.class, SocketAddress.class, MetricsConnection.class},
                new Object[]{conf, clusterId, localAddr, metrics});
    }
}
