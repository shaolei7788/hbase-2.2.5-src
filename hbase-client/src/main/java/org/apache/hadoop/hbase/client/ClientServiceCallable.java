/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

/**
 * A RegionServerCallable set to use the Client protocol.
 * Also includes some utility methods so can hide protobuf references here rather than have them
 * sprinkled about the code base.
 *
 * @param <T>
 */
@InterfaceAudience.Private
public abstract class ClientServiceCallable<T> extends RegionServerCallable<T, ClientProtos.ClientService.BlockingInterface> {

    public ClientServiceCallable(Connection connection, TableName tableName, byte[] row, RpcController rpcController, int priority) {
        super(connection, tableName, row, rpcController, priority);
    }

    @Override
    protected void setStubByServiceName(ServerName serviceName) throws IOException {
        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         */
        setStub(getConnection().getClient(serviceName));
    }

    // Below here are simple methods that contain the stub and the rpcController.
    protected ClientProtos.GetResponse doGet(
            ClientProtos.GetRequest request) throws org.apache.hbase.thirdparty.com.google.protobuf.ServiceException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： getStub() = BlockingStub
         */
        return getStub().get(getRpcController(), request);
    }

    protected ClientProtos.MutateResponse doMutate(
            ClientProtos.MutateRequest request) throws org.apache.hbase.thirdparty.com.google.protobuf.ServiceException {

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         *   1、虽然经过层层递进的寻找，发现，返回的对象类型是：BlockingStub
         *   2、最终跳转到 RSRpcServices
         *
         *   现在在探查put方法的底层实现， put方法的服务端响应者是  master regionserver ?
         *   是发送给 RegionServer 的， 是怎么确定到底发送给那个 Regionserver ？
         *
         */
        return getStub().mutate(getRpcController(), request);
    }
}
