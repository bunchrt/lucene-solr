package org.apache.solr.common;


import com.codahale.metrics.Counter;
import org.apache.solr.common.util.SysStats;
import org.apache.solr.common.util.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class SolrThread extends Thread {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static AtomicInteger COUNT = new AtomicInteger();
  private final String name;

  private volatile ExecutorService executorService;

  private static Counter executors = Metrics.MARKS_METRICS.counter("solrthread_executors");

  private static Counter executorsReused = Metrics.MARKS_METRICS.counter("solrthread_executors_reused");

  public SolrThread(ThreadGroup group, Runnable r, String name) {
    super(group, r, name + '-' + COUNT.incrementAndGet());
    this.name = getName();
    setDaemon(true);
    Thread currentThread = Thread.currentThread();
    if (currentThread instanceof SolrThread) {
      ExecutorService service = ((SolrThread) currentThread).getExecutorService();
      if (service == null || service.isShutdown()) {
        createExecutorService();
      } else {
        executorsReused.inc();
        setExecutorService(service);
      }
    }


  }

  public void resetName() {
    setName(name);
  }

  private void setExecutorService(ExecutorService service) {
    this.executorService = service;
  }

  private void createExecutorService() {
    // log.info("createExecutorService CLASSLOADER={}", Thread.currentThread().getClass().getClassLoader());
    executors.inc();
    this.executorService = ParWork.getExecutorService(name, Integer.getInteger("solr.perThreadPoolSize", SysStats.PROC_COUNT), false);
    // log.info("createExecutorService Class instance CLASSLOADER={}", executorService.getClass().getClassLoader());
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public static SolrThread getCurrentThread() {
    return (SolrThread) currentThread();
  }

  public void clearExecutor() {
    this.executorService = null;
  }

  //  public interface CreateThread  {
  //     SolrThread getCreateThread();
  //  }
}
