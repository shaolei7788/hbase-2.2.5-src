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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.util.ServerCommandLine;

/**
 * Class responsible for parsing the command line and starting the
 * RegionServer.
 */
@InterfaceAudience.Private
public class HRegionServerCommandLine extends ServerCommandLine {
    private static final Logger LOG = LoggerFactory.getLogger(HRegionServerCommandLine.class);

    private final Class<? extends HRegionServer> regionServerClass;

    private static final String USAGE = "Usage: HRegionServer [-D conf.param=value] start";

    public HRegionServerCommandLine(Class<? extends HRegionServer> clazz) {
        this.regionServerClass = clazz;
    }

    @Override
    protected String getUsage() {
        return USAGE;
    }

    private int start() throws Exception {
        Configuration conf = getConf();
        TraceUtil.initTracer(conf);
        try {

            // TODO_MA 注释：本地模式，在启动 Master 的时候，就已经启动了 Master 和 RegionServer 在同一个节点了。
            // If 'local', don't start a region server here. Defer to
            // LocalHBaseCluster. It manages 'local' clusters.
            if(LocalHBaseCluster.isLocal(conf)) {
                LOG.warn("Not starting a distinct region server because " + HConstants.CLUSTER_DISTRIBUTED + " is false");
            } else {

                // TODO_MA 注释：输出当前 JVM 的一些进程信息
                logProcessInfo(getConf());

                /********
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *   注释： 启动 Regionserver
                 *   1、通过反射构造 HRegionServer, 注意去看 HRegionServer 的带 Configuration 参数的构造方法，做了很多事情
                 *   2、启动 HRegionServer
                 */
                HRegionServer hrs = HRegionServer.constructRegionServer(regionServerClass, conf);
                hrs.start();
                hrs.join();

                if(hrs.isAborted()) {
                    throw new RuntimeException("HRegionServer Aborted");
                }
            }
        } catch(Throwable t) {
            LOG.error("Region server exiting", t);
            return 1;
        }
        return 0;
    }

    @Override
    public int run(String args[]) throws Exception {
        if(args.length != 1) {
            usage(null);
            return 1;
        }

        String cmd = args[0];

        /********
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 启动 Regionserver
         */
        if("start".equals(cmd)) {
            return start();

        } else if("stop".equals(cmd)) {
            System.err.println(
                    "To shutdown the regionserver run " + "hbase-daemon.sh stop regionserver or send a kill signal to " + "the regionserver pid");
            return 1;
        } else {
            usage("Unknown command: " + args[0]);
            return 1;
        }
    }
}
