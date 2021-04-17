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
package org.apache.solr.cloud;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

@Slow
@LuceneTestCase.Nightly // this test can randomly get stuck waiting a bit after finishing successfully while it waits for prep recovery op to time out
// I think waitForActiveCollection prob has a little hole on startup where it sees stale state and a live ephem node.
public class RecoveryZkTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeRecoveryZkTest() throws Exception {
    useFactory(null);
    System.setProperty("solr.skipCommitOnClose", "false");

    System.setProperty("solr.http2solrclient.default.idletimeout", "10000");
    System.setProperty("distribUpdateSoTimeout", "10000");
    System.setProperty("socketTimeout", "10000");
    System.setProperty("connTimeout", "10000");
    System.setProperty("solr.test.socketTimeout.default", "10000");
    System.setProperty("solr.connect_timeout.default", "10000");
    System.setProperty("solr.so_commit_timeout.default", "10000");
    System.setProperty("solr.httpclient.defaultConnectTimeout", "10000");
    System.setProperty("solr.httpclient.defaultSoTimeout", "10000");

    configureCluster(2).formatZk(true)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
  }


  @AfterClass
  public static void afterRecoveryZkTest() throws Exception {
    shutdownCluster();
  }

  private final List<StoppableIndexingThread> threads = Collections.synchronizedList(new ArrayList<>());

  @After
  public void stopThreads() throws InterruptedException {
    for (StoppableIndexingThread t : threads) {
      t.safeStop();
    }

    threads.clear();
  }

  @Test
  //commented 2-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 28-June-2018
  public void test() throws Exception {

    final String collection = "recoverytest";

    CollectionAdminRequest.createCollection(collection, "conf", 1, 2)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    cluster.getSolrClient().setDefaultCollection(collection);

    // start a couple indexing threads
    
    int[] maxDocList = new int[] {25, 55};
    int[] maxDocNightlyList = new int[] {3000, 7000, 12000, 30000, 45000, 60000};
    
    int maxDoc;
    if (!TEST_NIGHTLY) {
      maxDoc = maxDocList[random().nextInt(maxDocList.length - 1)];
    } else {
      maxDoc = maxDocNightlyList[random().nextInt(maxDocList.length - 1)];
    }
    log.info("Indexing {} documents", maxDoc);
    
    final StoppableIndexingThread indexThread
      = new StoppableIndexingThread(null, cluster.getSolrClient(), "1", true, maxDoc, 1, true);
    threads.add(indexThread);
    Future<?> future = ParWork.submit("StoppableSearchThread", indexThread);
    
    final StoppableIndexingThread indexThread2
      = new StoppableIndexingThread(null, cluster.getSolrClient(), "2", true, maxDoc, 1, true);
    threads.add(indexThread2);
    Future<?> future2 = ParWork.submit("StoppableSearchThread", indexThread2);

    // give some time to index...
    int[] waitTimes;
    if (TEST_NIGHTLY) {
      waitTimes = new int[]{1000, 2000, 5000};
    } else {
      waitTimes = new int[]{250, 350, 500};
    }
    Thread.sleep(waitTimes[random().nextInt(waitTimes.length - 1)]);
     
    // bring shard replica down
    DocCollection state = getCollectionState(collection);
    Replica leader = state.getLeader("s1");
    Replica replica = getRandomReplica(state.getSlice("s1"), (r) -> leader != r);

    JettySolrRunner jetty = cluster.getReplicaJetty(replica);
    jetty.stop();
    
    // wait a moment - lets allow some docs to be indexed so replication time is non 0
    Thread.sleep(waitTimes[random().nextInt(waitTimes.length - 1)]);
    
    // bring shard replica up
    log.info("bring jetty up");
    jetty.start();

    // stop indexing threads
    indexThread.safeStop();
    indexThread2.safeStop();
    
    future.get();
    future2.get();

    cluster.waitForActiveCollection(collection, 1, 2);

    // test that leader and replica have same doc count

    int cnt = 0;
    while (true) {
      try {
        new UpdateRequest().commit(cluster.getSolrClient(), collection);
      } catch (Exception e) {
        log.info("commit fail", e);
      }

      try {
        state = getCollectionState(collection);
        assertShardConsistency(state.getSlice("s1"), true);
        break;
      } catch (AssertionError error) {
        if (cnt++ > 5) {
          throw error;
        }
      }
      Thread.sleep(500);
    }
  }

  private void assertShardConsistency(Slice shard, boolean expectDocs) throws Exception {
    List<Replica> replicas = shard.getReplicas(r -> r.getState() == Replica.State.ACTIVE);
    long[] numCounts = new long[replicas.size()];
    int i = 0;
    for (Replica replica : replicas) {
      try (Http2SolrClient client = new Http2SolrClient.Builder(replica.getCoreUrl())
          .withHttpClient(cluster.getSolrClient().getHttpClient()).build()) {
        numCounts[i] = client.query(new SolrQuery("*:*").add("distrib", "false")).getResults().getNumFound();
        i++;
      }
    }
    for (int j = 1; j < replicas.size(); j++) {
      if (numCounts[j] != numCounts[j - 1])
        fail("Mismatch in counts between replicas replica1=" + numCounts[j] + " replica2=" + numCounts[j - 1]);  // TODO improve this!
      if (numCounts[j] == 0 && expectDocs)
        fail("Expected docs on shard " + shard.getName() + " but found none");
    }
  }

}
