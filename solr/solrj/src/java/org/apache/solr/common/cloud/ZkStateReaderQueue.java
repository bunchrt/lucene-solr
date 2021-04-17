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
package org.apache.solr.common.cloud;

import com.codahale.metrics.Meter;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.ParWorkExecutor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.metrics.Metrics;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

public class ZkStateReaderQueue implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Meter stateUpdateRequests = Metrics.MARKS_METRICS.meter("zkreader_stateupdates_requests");
  private static final Meter docCollUpdateRequests = Metrics.MARKS_METRICS.meter("zkreader_doccollupdates_requests");

  private final static FetchStateUpdatesRequest TERMINATED = new FetchStateUpdatesRequest(null,null, false);

  private final LinkedTransferQueue<FetchStateUpdatesRequest> workQueue = new LinkedTransferQueue<>();

  private final SolrZkClient zkClient;
  private final ZkStateReader reader;

  private volatile Worker worker;

  private volatile boolean terminated;
  private volatile boolean closed;

  private volatile ExecutorService workerExec;


  public ZkStateReaderQueue(ZkStateReader reader) {
   this.zkClient = reader.getZkClient();
   this.reader = reader;
  }

  public void close() {
    this.closed = true;

    //    if (!workQueue.tryTransfer(TERMINATED)) {
    workQueue.put(TERMINATED);
    //    }

    workerExec.shutdown();
    try {
      workerExec.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {

    }
  }

  private static class BulkMessage extends ConcurrentHashMap<String, Set<FetchStateUpdatesRequest>> {

  }

  private class Worker implements Runnable {

    public static final int POLL_TIME_ON_PUBLISH_NODE = 1;
    public static final int POLL_TIME = 250;

    Worker() {

    }

    // section worker run
    @Override public void run() {
      MDCLoggingContext.setNode(reader.node);
      while (!terminated && !closed) {
        try {

          FetchStateUpdatesRequest updateRequest = null;
          try {
            log.debug("ZkStateReaderQueue worker will poll for 5 seconds");
            updateRequest = workQueue.poll(5000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            updateRequest = TERMINATED;
            terminated = true;
            terminated = true;
          } catch (Exception e) {
            log.warn("state publisher hit exception polling", e);
          }
          BulkMessage bulkMessage = null;
          if (updateRequest != null) {
            log.debug("got state update request collection={} structureUpdateToo={}", updateRequest.collection, !updateRequest.justStates);
            bulkMessage = new BulkMessage();

            int pollTime;
            if (updateRequest == TERMINATED) {
              terminated = true;
              pollTime = 0;
            } else {
              pollTime = bulkMessage(updateRequest, bulkMessage);
            }

            while (true) {
              try {
                updateRequest = workQueue.poll(pollTime, TimeUnit.MILLISECONDS);

              } catch (InterruptedException e) {
                updateRequest = TERMINATED;
                terminated = true;
              } catch (Exception e) {
                log.warn("ZkStateReaderQueue hit exception polling", e);
              }
              if (updateRequest != null) {
                log.debug("got state update request collection={} structureUpdateToo={}", updateRequest.collection);
                if (updateRequest == TERMINATED) {
                  terminated = true;
                  pollTime = 0;
                } else {
                  pollTime = bulkMessage(updateRequest, bulkMessage);
                }
              } else {
                break;
              }
            }
          }

          if (bulkMessage != null && bulkMessage.size() > 0) {
            process(bulkMessage);
          }

        } catch (Exception e) {
          log.error("Exception in ZkStateReaderQueue Worker run loop", e);
        }

      }
      log.info("ZkStateReaderQueue has terminated");
    }

    private void process(BulkMessage bulkMessage) {
      try {
        log.debug("process state update requests {}", bulkMessage);

        bulkMessage.forEach((collection, fetchStateUpdatesRequests) -> {
          boolean justStates = true;
          for (FetchStateUpdatesRequest fetch : fetchStateUpdatesRequests) {
            if (!fetch.justStates) {
              justStates = false;
              break;
            }
          }

          try {
            if (!justStates) {
              log.debug("fetchCollectionState {}", collection);
              fetchCollectionState(collection).thenAcceptAsync(docCollection1 -> {
                reader.updateWatchedCollection(collection, new ClusterState.CollectionRef(docCollection1));
              });

            } else {
              log.debug("getAndProcessStateUpdates {}", collection);
              getAndProcessStateUpdates(reader.watchedCollectionStates.get(collection)).thenAcceptAsync(docCollection1 -> {
                reader.updateWatchedCollection(collection, new ClusterState.CollectionRef(docCollection1));
              });
            }
          } catch (Exception e) {
            log.error("Exception processing state update request", e);
            fetchStateUpdatesRequests.forEach(fetchStateUpdatesRequest -> fetchStateUpdatesRequest.future.completeExceptionally(e));
          }
        });

      } catch (Exception e) {
        log.error("Exception processing state update request", e);
      }
    }

    private int bulkMessage(FetchStateUpdatesRequest fetchStateUpdatesRequest, BulkMessage bulkMessage) {
      if (fetchStateUpdatesRequest == TERMINATED) {
        return 1;
      }

      Set<FetchStateUpdatesRequest> currentSet = bulkMessage.computeIfAbsent(fetchStateUpdatesRequest.collection, integer -> ConcurrentHashMap.newKeySet());
      currentSet.add(fetchStateUpdatesRequest);
      return 20;
    }
  }

  public void start() {
    if (worker != null) {
      return;
    }
    this.worker = new Worker();

    workerExec = Executors.newSingleThreadExecutor(new SolrNamedThreadFactory("ZkStateReaderQueue", true));

    workerExec.submit(this.worker);
  }

  public CompletableFuture<DocCollection> getAndProcessStateUpdates(DocCollection docCollection) {
    stateUpdateRequests.mark();
    try {
      if (log.isDebugEnabled()) {
        log.debug("get and process state updates for {}", docCollection);
      }

      if (docCollection == null) {
        log.debug("Null docCollection as argument");
        return CompletableFuture.completedFuture(null);
      }

      //      Stat stat;
      //      try {
      //        stat = getZkClient().exists(stateUpdatesPath, null, true, false);
      //        if (stat == null) {
      //          log.debug("stateUpdatesPath not found {}", stateUpdatesPath);
      //          return docCollection;
      //        }
      //      } catch (NoNodeException e) {
      //        log.info("No node found for {}", stateUpdatesPath);
      //        return docCollection;
      //      }
      //
      //      Integer oldVersion = docCollection.getStateUpdatesZkVersion();
      //      if (stat.getVersion() < oldVersion) {
      //        if (log.isDebugEnabled())
      //          log.debug("Will not apply state updates based on updates znode, they are for an older set of updates {}, ours is now {}", stat.getVersion(),
      //              oldVersion);
      //        return docCollection;
      //      }

      return CompletableFuture.supplyAsync(() -> {

        String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(docCollection.getName());
        byte[] data = new byte[0];
        Stat stat2 = new Stat();

        try {
          data = zkClient.getData(stateUpdatesPath, null, stat2, true, false);
        } catch (KeeperException e) {
          log.error("Exception fetching state updates from ZK", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception fetching state updates from ZK", e);
        } catch (InterruptedException e) {
          log.error("Exception fetching state updates from ZK", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception fetching state updates from ZK", e);
        }

        if (data == null) {
          log.debug("No data found for {}", stateUpdatesPath);
          return docCollection;
        }

        Map<Integer,Integer> m = null;
        try {
          m = Collections.unmodifiableMap(new LinkedHashMap<>((Map) Utils.fromJavabin(data)));
        } catch (IOException e) {
          log.error("Exception parsing return data for state updates", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception parsing return data for state updates", e);
        }
        log.debug("Got additional state updates {}", m);

        if (m.size() == 0) {
          log.debug("No updates found at {} {} {} {}", stateUpdatesPath, data.length, new String(data), stat2.getVersion());
          return docCollection;
        }

        log.debug("Got additional state updates for {} with znode version {} current DocCollection version is {} updates={}", docCollection, stat2.getVersion(),
            docCollection.getStateUpdatesZkVersion(), m);

        int stateUpdatesVersion = stat2.getVersion();

        if (stateUpdatesVersion <= docCollection.getStateUpdatesZkVersion()) {
          log.debug("Will not apply state updates based on state state updates version, they are for an older state.json {}, ours is now {}",
              stat2.getVersion(), docCollection.getStateUpdatesZkVersion());
          return docCollection;
        }

        Set<Map.Entry<Integer,Integer>> entrySet = m.entrySet();

        for (Map.Entry<Integer,Integer> entry : entrySet) {
          if (log.isTraceEnabled()) {
            log.trace("update state for {} replica id={} state={} zkversion={}", docCollection.toString(), entry.getKey(), entry.getValue(),
                stateUpdatesVersion);
          }

          Integer state = entry.getValue();

          if (state == 1) {
            Replica replica = docCollection.getReplicaById(entry.getKey());
            if (replica != null) {
              String sliceName = replica.getSlice();
              if (sliceName != null) {
                Slice slice = docCollection.getSlice(sliceName);
                if (slice != null) {
                  Collection<Replica> replicas = slice.getReplicas();
                  for (Replica r : replicas) {
                    if (r.getInternalId() != entry.getKey() && r.getRawState() == Replica.State.LEADER) {
                      docCollection.updateState(r.getInternalId(), 2);
                    }
                  }
                }
              }
            }
          }

          docCollection.updateState(entry.getKey(), entry.getValue());
        }

        log.debug("finished a new doc state based on state update diff {}", docCollection);

        docCollection.setStateUpdatesZkVersion(stateUpdatesVersion);
        return docCollection;
      }, ParWork.getRootSharedExecutor()); // uses common pool, no IO
    } catch (Exception e) {
      log.error("{} exception trying to process additional state updates", docCollection.getName(), e);
      return CompletableFuture.failedFuture(new SolrException(SolrException.ErrorCode.SERVER_ERROR, e));
    }
  }

  public void fetchStateUpdates(String collection, boolean justStates) {
    log.debug("add update request to queue for collection={}", collection);
//    if (closed) {
//      throw new AlreadyClosedException();
//    }

    if (collection == null) {
      log.error("null collection name passed to fetchStateUpdates");
      throw new IllegalArgumentException();
    }

    FetchStateUpdatesRequest request = new FetchStateUpdatesRequest(collection,  null, justStates);

   // if (!workQueue.tryTransfer(request)) {
      workQueue.put(request);
  //  }

  }

  // region fetch
  CompletableFuture<DocCollection> fetchCollectionState(String collection) {
    String collectionPath = reader.getCollectionPath(collection);
    if (log.isDebugEnabled()) log.debug("Looking at fetching full clusterstate collection={}", collection);

    docCollUpdateRequests.mark();

    log.debug("getting latest state.json for {}", collection);

    return CompletableFuture.supplyAsync(() -> {
      try {
        Stat stat = new Stat();
        byte[] data = zkClient.getData(collectionPath, null, stat);
        return ClusterState.createDocCollectionFromJson(stat.getVersion(), data);

      } catch (KeeperException.NoNodeException e) {
        log.debug("no state.json znode found");
        return null;
      } catch (Exception e) {
        log.debug("Exception getting and parsing state.json");
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception getting and parsing state.json", e);
      }
    }, ParWork.getRootSharedExecutor()).thenCompose(docCollection -> getAndProcessStateUpdates(docCollection));
  }

//  private void processDocCollection(FetchStateUpdatesRequest fetchStateUpdatesRequest, DocCollection docCollection) {
//    try {
//      MDCLoggingContext.setNode(reader.node);
//
//      if (log.isDebugEnabled()) {
//        log.debug("process doc collection docCollection={}", docCollection);
//        if (docCollection == null) {
//          log.debug("null docState", new RuntimeException());
//        }
//      }
//
//      DocCollection finalDocCollection = docCollection;
//      fetchStateUpdatesRequest.future.complete(finalDocCollection);
//
//      if (reader.updateWatchedCollection(fetchStateUpdatesRequest.collection, new ClusterState.CollectionRef(docCollection))) {
//        reader.notifyStateUpdated(fetchStateUpdatesRequest.collection, docCollection, "state.json watcher");
//      }
//
//    } catch (Exception e) {
//      log.error("Failed processing state update fetch for collection={}", fetchStateUpdatesRequest.collection, e);
//      fetchStateUpdatesRequest.future.completeExceptionally(e);
//      return;
//    }
//  }

  static class FetchStateUpdatesRequest {
    final String collection;
    volatile CompletableFuture<DocCollection> future;
    final boolean justStates;

    public FetchStateUpdatesRequest(String collection, CompletableFuture<DocCollection> docCollectionCompletableFuture, boolean justStates) {
      this.collection = collection;
      this.future = docCollectionCompletableFuture;
      this.justStates = justStates;
    }
  }

}
