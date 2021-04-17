/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr;

import org.apache.lucene.search.TimeLimitingCollector.TimerThread;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import org.apache.solr.common.ParWork;
import org.apache.zookeeper.server.SessionTracker;

/**
 * This ignores those threads in Solr for which there is no way to
 * clean up after a suite.
 */
public class SolrIgnoredThreadsFilter implements ThreadFilter {
  @Override
  public boolean reject(Thread t) {


    /*
     * IMPORTANT! IMPORTANT!
     * 
     * Any threads added here should have ABSOLUTELY NO SIDE EFFECTS
     * (should be stateless). This includes no references to cores or other
     * test-dependent information.
     */

    String threadName = t.getName();
//
//
//    if (threadName.startsWith("ParWorkPerThread")) { // MRM tmp
//      return true;
//    }
//
//    if (threadName.equals(TimerThread.THREAD_NAME)) {
//      return true;
//    }
//
//    // due to netty - will stop on it's own
////    if (threadName.startsWith("globalEventExecutor")) {
////      return true;
////    }
//
//    // These is a java pool for the collection stream api
    if (threadName.startsWith("ForkJoinPool.")) {
      return true;
    }

    if (threadName.startsWith("pool-")) {
      return true;
    }

    if (threadName.startsWith(ParWork.ROOT_EXEC_NAME)) {
      return true;
    }

//
//    // the jetty reserved executor
    if (threadName.startsWith("NIOWorkerThread-")) {
      return true;
    }
//
//    // zookeeper thread that can take a moment to stop
    if (threadName.startsWith("SessionTracker")) {
      return true;
    }
    if (threadName.startsWith("SyncThread")) {
      return true;
    }

//
//    // HDFS MRM TODO: fix
////    if (threadName.startsWith("IPC Parameter Sending Thread ")) { // SOLR-5007
////      return true;
////    } if (threadName.startsWith("IPC Client")) { // SOLR-5007
////      return true;
////    } else if (threadName.startsWith("org.apache.hadoop.hdfs.PeerCache")) { // SOLR-7288
////      return true;
////    } else if (threadName.endsWith("StatisticsDataReferenceCleaner")) {
////      return true;
////    } else if (threadName.startsWith("LeaseRenewer")) { // SOLR-7287
////      return true;
////    } else if (threadName.startsWith("org.apache.hadoop.fs.FileSystem$Statistics")) { // SOLR-11261
////      return true;
////    } else if (threadName.startsWith("ForkJoinPool.")) { // JVM built in pool
////      return true;
////    } else if (threadName.startsWith("solr-hdfs-threadpool-")) { // SOLR-9515 and HDFS-14251
////      return true;
////    } else if (threadName.startsWith("nioEventLoopGroup")) { // Netty threads from hdfs
////      return true;
////    }
//
    return true;
  }
}
