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

package org.apache.solr.update;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.update.PeerSync.MissedUpdatesRequest;
import static org.apache.solr.update.PeerSync.absComparator;
import static org.apache.solr.update.PeerSync.percentile;

public class PeerSyncWithLeader implements SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean debug = log.isDebugEnabled();

  private String leaderUrl;
  private int nUpdates;

  private UpdateHandler uhandler;
  private UpdateLog ulog;
  private Http2SolrClient clientToLeader;

  private final boolean doFingerprint;

  private SolrCore core;
  private PeerSync.Updater updater;
  private MissedUpdatesFinder missedUpdatesFinder;
  private Set<Long> bufferedUpdates;

  // metrics
  private Timer syncTime;
  private Counter syncErrors;
  private Counter syncSkipped;
  private SolrMetricsContext solrMetricsContext;

  public PeerSyncWithLeader(SolrCore core, String leaderUrl, int nUpdates) {
    this.core = core;
    this.leaderUrl = leaderUrl;
    this.nUpdates = nUpdates;

    this.doFingerprint = !"true".equals(System.getProperty("solr.disableFingerprint"));
    this.uhandler = core.getUpdateHandler();
    this.ulog = uhandler.getUpdateLog();

    this.clientToLeader = new Http2SolrClient.Builder(leaderUrl).withHttpClient(core
        .getCoreContainer().getUpdateShardHandler().
        getLeaderCheckClient()).markInternalRequest().build();

    this.updater = new PeerSync.Updater(msg(), core);

    core.getCoreMetricManager().registerMetricProducer(SolrInfoBean.Category.REPLICATION.toString(), this);
  }

  public static final String METRIC_SCOPE = "peerSync";

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    this.solrMetricsContext = parentContext.getChildContext(this);
    syncTime = solrMetricsContext.timer("time", scope, METRIC_SCOPE);
    syncErrors = solrMetricsContext.counter("errors", scope, METRIC_SCOPE);
    syncSkipped = solrMetricsContext.counter("skipped", scope, METRIC_SCOPE);
  }

  // start of peersync related debug messages.  includes the core name for correlation.
  private String msg() {
    ZkController zkController = uhandler.core.getCoreContainer().getZkController();
    String myURL = "";
    if (zkController != null) {
      myURL = zkController.getBaseUrl();
    }

    return "PeerSync: core="+uhandler.core.getName()+ " url="+myURL +" ";
  }

  public void close() {
    IOUtils.closeQuietly(clientToLeader);
  }

  /**
   * Sync with leader
   * @param startVersions : recent versions on startup
   * @return result of PeerSync with leader
   */
  public PeerSync.PeerSyncResult sync(List<Long> startVersions){
    if (ulog == null) {
      syncErrors.inc();
      return PeerSync.PeerSyncResult.failure();
    }

    ArrayList<Long> startingVersions = new ArrayList<>(startVersions);

    NamedList<Object> leaderVersionsAndFingerprint = null;
    if (startingVersions.isEmpty()) {
      leaderVersionsAndFingerprint = getVersions();

      IndexFingerprint fingerPrint = getFingerprint(leaderVersionsAndFingerprint);
      List<Long> otherVersions = (List<Long>)leaderVersionsAndFingerprint.get("versions");

//      if ((fingerPrint != null && fingerPrint.getMaxDoc() == 0) && (otherVersions == null || otherVersions.size() == 0)) {
//
//        RefCounted<SolrIndexSearcher> rtsh = core.getRealtimeSearcher();
//        try {
//          SolrIndexSearcher rts = rtsh.get();
//          if (rts.maxDoc() == 0) {
//            return PeerSync.PeerSyncResult.success();
//          } else {
//            log.warn("no frame of reference to tell if we've missed updates {} {}", fingerPrint, otherVersions);
//            return PeerSync.PeerSyncResult.failure();
//          }
//        } finally {
//          rtsh.decref();
//        }
//
//      } else {
        log.warn("no frame of reference to tell if we've missed updates {} {}", fingerPrint, otherVersions);
        syncErrors.inc();
        return PeerSync.PeerSyncResult.failure();
//      }
    }

    Timer.Context timerContext = null;
    try {
      if (log.isInfoEnabled()) {
        log.info("{} START leader={} nUpdates={}", msg(), leaderUrl, nUpdates);
      }

      if (debug) {
        log.debug("{} startingVersions={} {}", msg(), startingVersions.size(), startingVersions);
      }
      // check if we already in sync to begin with
      if(doFingerprint && alreadyInSync()) {
        syncSkipped.inc();
        return PeerSync.PeerSyncResult.success();
      }

      // measure only when actual sync is performed
      timerContext = syncTime.time();

      List<Long> ourUpdates;
      try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
        ourUpdates = recentUpdates.getVersions(nUpdates);
        bufferedUpdates = recentUpdates.getBufferUpdates();
      }

      ourUpdates.sort(absComparator);
      startingVersions.sort(absComparator);

      long ourLowThreshold = percentile(startingVersions, 0.8f);
      long ourHighThreshold = percentile(startingVersions, 0.2f);

      // now make sure that the starting updates overlap our updates
      // there shouldn't be reorders, so any overlap will do.
      long smallestNewUpdate = 0;
      if (ourUpdates.size() > 0) {
        smallestNewUpdate = Math.abs(ourUpdates.get(ourUpdates.size() - 1));
      }

      if (!startingVersions.isEmpty() && Math.abs(startingVersions.get(0)) < smallestNewUpdate) {
        log.warn("{} too many updates received since start - startingUpdates no longer overlaps with our currentUpdates", msg());
        syncErrors.inc();
        return PeerSync.PeerSyncResult.failure();
      }

      // let's merge the lists
      for (Long ver : startingVersions) {
        if (Math.abs(ver) < smallestNewUpdate) {
          ourUpdates.add(ver);
        }
      }

      boolean success = doSync(ourUpdates, ourLowThreshold, ourHighThreshold, leaderVersionsAndFingerprint);

      if (log.isDebugEnabled()) {
        log.debug("{} DONE. sync {}", msg(), (success ? "succeeded" : "failed"));
      }
      if (!success) {
        syncErrors.inc();
      }
      return success ?  PeerSync.PeerSyncResult.success() : PeerSync.PeerSyncResult.failure();
    } finally {
      if (timerContext != null) {
        timerContext.close();
      }
    }
  }

  private boolean doSync(List<Long> ourUpdates, long ourLowThreshold, long ourHighThreshold, NamedList<Object> leaderVersionsAndFingerprint) {
    // get leader's recent versions and fingerprint
    // note: by getting leader's versions later, we guarantee that leader's versions always super set of {@link bufferedUpdates}
    if (leaderVersionsAndFingerprint == null) {
      leaderVersionsAndFingerprint = getVersions();
    }
    IndexFingerprint leaderFingerprint = getFingerprint(leaderVersionsAndFingerprint);
    if (doFingerprint) {
      if (leaderFingerprint == null) {
        log.warn("Could not get fingerprint from the leader");
        return false;
      }
      log.debug("Leader fingerprint {}", leaderFingerprint);
    }

    missedUpdatesFinder = new MissedUpdatesFinder(ourUpdates, msg(), nUpdates, ourLowThreshold);
    MissedUpdatesRequest missedUpdates = buildMissedUpdatesRequest(leaderVersionsAndFingerprint);
    if (missedUpdates == MissedUpdatesRequest.ALREADY_IN_SYNC) return true;
    if (missedUpdates != MissedUpdatesRequest.UNABLE_TO_SYNC) {
      NamedList<Object> missedUpdatesRsp = requestUpdates(missedUpdates);
      if (handleUpdates(missedUpdatesRsp, missedUpdates.totalRequestedUpdates, leaderFingerprint)) {
        if (doFingerprint) {
          return compareFingerprint(leaderFingerprint);
        }
        return true;
      }
    }
    return false;
  }

  private MissedUpdatesRequest buildMissedUpdatesRequest(NamedList<Object> rsp) {
    // we retrieved the last N updates from the replica
    @SuppressWarnings({"unchecked"})
    List<Long> otherVersions = (List<Long>)rsp.get("versions");
    if (log.isDebugEnabled()) {
      log.debug("{} Received {} versions from {}", msg(), otherVersions.size(), leaderUrl);
    }

    if (otherVersions.isEmpty()) {
      return MissedUpdatesRequest.UNABLE_TO_SYNC;
    }

    MissedUpdatesRequest updatesRequest = missedUpdatesFinder.find(otherVersions, leaderUrl, () -> core.getSolrConfig().useRangeVersionsForPeerSync);
    if (updatesRequest == MissedUpdatesRequest.EMPTY) {
      if (doFingerprint && updatesRequest.totalRequestedUpdates > 0) return MissedUpdatesRequest.UNABLE_TO_SYNC;
      return MissedUpdatesRequest.ALREADY_IN_SYNC;
    }

    return updatesRequest;
  }

  private NamedList<Object> requestUpdates(MissedUpdatesRequest missedUpdatesRequest) {

    if (log.isDebugEnabled()) log.debug("{} Requesting updates from {} n={} versions={}", msg(), leaderUrl
          , missedUpdatesRequest.totalRequestedUpdates, missedUpdatesRequest.versionsAndRanges);


    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/get");
    params.set(DISTRIB, false);
    params.set("getUpdates", missedUpdatesRequest.versionsAndRanges);
    params.set("onlyIfActive", false);
    params.set("onlyIfLeader", true);
    params.set("skipDbq", true);

    return request(params, "Failed on getting missed updates from the leader");
  }

  private boolean handleUpdates(NamedList<Object> rsp, long numRequestedUpdates, IndexFingerprint leaderFingerprint) {
    // missed updates from leader, it does not contains updates from bufferedUpdates
    @SuppressWarnings({"unchecked"})
    List<Object> updates = (List<Object>)rsp.get("updates");

    if (updates.size() < numRequestedUpdates) {
      if (log.isDebugEnabled()) log.debug("{} Requested {} updated from {} but retrieved {}", msg(), numRequestedUpdates, leaderUrl, updates.size());
      return false;
    }

    // by apply buffering update, replica will have fingerprint equals to leader.
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      for (Long bufferUpdate : bufferedUpdates) {
        // updater will sort updates before apply
        updates.add(recentUpdates.lookup(bufferUpdate));
      }
    }

    // Leader will compute its fingerprint, then retrieve its recent updates versions.
    // There are a case that some updates (gap) get into recent versions but do not exist in index (fingerprint).
    // If the gap do not contains DBQ or DBI, it is safe to use leaderFingerprint.maxVersionEncountered as a cut point.
    // TODO leader should do fingerprint and retrieve recent updates version in atomic
    if (leaderFingerprint != null) {
      boolean existDBIOrDBQInTheGap = updates.stream().anyMatch(e -> {
        @SuppressWarnings({"unchecked"})
        List<Object> u = (List<Object>) e;
        long version = (Long) u.get(1);
        int oper = (Integer)u.get(0) & UpdateLog.OPERATION_MASK;
        // only DBI or DBQ in the gap (above) will satisfy this predicate
        return version > leaderFingerprint.getMaxVersionEncountered() && (oper == UpdateLog.DELETE || oper == UpdateLog.DELETE_BY_QUERY);
      });
      if (log.isDebugEnabled()) log.debug("existDBIOrDBQInTheGap={}", existDBIOrDBQInTheGap);
      if (!existDBIOrDBQInTheGap) {
        // it is safe to use leaderFingerprint.maxVersionEncountered as cut point now.
        updates.removeIf(e -> {
          @SuppressWarnings({"unchecked"})
          List<Object> u = (List<Object>) e;
          long version = (Long) u.get(1);
          boolean success = version > leaderFingerprint.getMaxVersionEncountered();
          if (log.isDebugEnabled()) log.debug("existDBIOrDBQInTheGap version={}  leaderFingerprint.getMaxVersionEncountered={} success={}", version, leaderFingerprint.getMaxVersionEncountered(), success);
          return success;
        });
      }
    }

    try {
      updater.applyUpdates(updates, leaderUrl);
    } catch (Exception e) {
      log.error("Could not apply updates", e);
      return false;
    }
    return true;
  }

  private NamedList<Object> request(ModifiableSolrParams params, String onFail) {
    try {
      QueryRequest qr = new QueryRequest(params, SolrRequest.METHOD.POST);
      QueryResponse rsp = qr.process(clientToLeader);
      Exception exception = rsp.getException();
      if (exception != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, onFail, exception);
      }
      return rsp.getResponse();
    } catch (SolrServerException | IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, onFail, e);
    }
  }

  private NamedList<Object> getVersions() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt","/get");
    params.set(DISTRIB,false);
    params.set("getVersions",nUpdates);
    params.set("fingerprint",doFingerprint);
    params.set("onlyIfLeader", true);

    return request(params, "Failed to get recent versions from leader");
  }

  private boolean alreadyInSync() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/get");
    params.set(DISTRIB,false);
    params.set("getFingerprint", String.valueOf(Long.MAX_VALUE));
    params.set("onlyIfLeader", true);

    NamedList<Object> rsp = request(params, "Failed to get fingerprint from leader");

    IndexFingerprint leaderFingerprint = getFingerprint(rsp);
//    if (leaderFingerprint != null && leaderFingerprint.getMaxDoc() == 0) {
//
//      RefCounted<SolrIndexSearcher> rtsh = core.getRealtimeSearcher();
//      try {
//        SolrIndexSearcher rts = rtsh.get();
//        return rts.maxDoc() == 0;
//      } finally {
//        rtsh.decref();
//      }
//    }

    return compareFingerprint(leaderFingerprint);
  }

  private IndexFingerprint getFingerprint(NamedList<Object> rsp) {
    Object fingerprint = null;
    if (rsp != null) fingerprint = rsp.get("fingerprint");
    if (fingerprint == null) return null;
    return IndexFingerprint.fromObject(fingerprint);
  }

  private boolean compareFingerprint(IndexFingerprint leaderFingerprint) {
    if (leaderFingerprint == null) {
      log.warn("Replica did not return a fingerprint - possibly an older Solr version or exception");
      return false;
    }

    try {
      IndexFingerprint ourFingerprint = IndexFingerprint.getFingerprint(core, Long.MAX_VALUE);
      int cmp = IndexFingerprint.compare(leaderFingerprint, ourFingerprint);
      if (log.isDebugEnabled()) log.debug("Fingerprint comparison result: {}" , cmp);
      if (cmp != 0) {
        if (log.isDebugEnabled()) log.debug("Leader fingerprint: {}, Our fingerprint: {}", leaderFingerprint , ourFingerprint);
      }

      return cmp == 0;  // currently, we only check for equality...
    } catch (IOException e) {
      log.warn("Could not confirm if we are already in sync. Continue with PeerSync");
    }
    return false;
  }

  /**
   * Helper class for doing comparison ourUpdates and other replicas's updates to find the updates that we missed
   */
  public static class MissedUpdatesFinder extends PeerSync.MissedUpdatesFinderBase {
    private long ourHighest;
    private String logPrefix;
    private long nUpdates;

    MissedUpdatesFinder(List<Long> ourUpdates, String logPrefix, long nUpdates,
                        long ourLowThreshold) {
      super(ourUpdates, ourLowThreshold);

      this.logPrefix = logPrefix;
      if (ourUpdates.size() > 0) {
        this.ourHighest = ourUpdates.get(0);
      }
      this.nUpdates = nUpdates;
    }

    public MissedUpdatesRequest find(List<Long> leaderVersions, Object updateFrom, Supplier<Boolean> canHandleVersionRanges) {
      leaderVersions.sort(absComparator);
      log.debug("{} sorted versions from {} = {}", logPrefix, leaderVersions, updateFrom);

      long leaderLowest = leaderVersions.get(leaderVersions.size() - 1);
      if (Math.abs(ourHighest) < Math.abs(leaderLowest)) {
        log.info("{} Our versions are too old comparing to leader, ourHighest={} otherLowest={}", logPrefix, ourHighest, leaderLowest);
        return MissedUpdatesRequest.UNABLE_TO_SYNC;
      }
      // we don't have to check the case we ahead of the leader.
      // (maybe we are the old leader and we contain some updates that no one have)
      // In that case, we will fail on compute fingerprint with the current leader and start segments replication

      boolean completeList = leaderVersions.size() < nUpdates;
      MissedUpdatesRequest updatesRequest;
      if (canHandleVersionRanges.get()) {
        updatesRequest = handleVersionsWithRanges(ourUpdates, leaderVersions, completeList, ourLowThreshold);
      } else {
        updatesRequest = handleIndividualVersions(leaderVersions, completeList);
      }

      if (updatesRequest.totalRequestedUpdates > nUpdates) {
        log.info("{} PeerSync will fail because number of missed updates is more than:{}", logPrefix, nUpdates);
        return MissedUpdatesRequest.UNABLE_TO_SYNC;
      }

      if (updatesRequest == MissedUpdatesRequest.EMPTY) {
        log.info("{} No additional versions requested", logPrefix);
      }

      return updatesRequest;
    }
  }

}
