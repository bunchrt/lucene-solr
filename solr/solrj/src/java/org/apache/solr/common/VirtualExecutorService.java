package org.apache.solr.common;

import org.apache.solr.client.solrj.impl.AsyncTracker;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/*
 * A virtual executor service that uses threads from the parent executor service.
 *
 * The parent service should use an unbounded queue or otherwise not reject tasks unless shutdown.
 *
 * Only ~ maxSize tasks will be allowed outstanding and waiting to execute at once on the parent thread pool, else they
 * will be run by the caller thread or block until room is available depending on the wait constructor argument. It is
 * expected that multiple instances of this class will share a parent executor.
 *
 * */
public class VirtualExecutorService extends AbstractExecutorService {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_AVAILABLE = Math.max(ParWork.PROC_COUNT / 2, 3);

  private final ParWorkExecutor service;
  private final String name;
  private volatile boolean terminated;
  private volatile boolean shutdown;

  private final AsyncTracker asyncTracker;

  public VirtualExecutorService(String name, ParWorkExecutor service, int maxSize, boolean wait) {
    assert service != null;
    this.name = name;
    this.service = service;
    asyncTracker = new AsyncTracker(maxSize, wait);
  }

  @Override public void shutdown() {
    assert ObjectReleaseTracker.getInstance().release(this);
    this.shutdown = true;
    IOUtils.closeQuietly(asyncTracker);
  }

  @Override public List<Runnable> shutdownNow() {
    shutdown = true;
    asyncTracker.close();
    return Collections.emptyList();
  }

  @Override public boolean isShutdown() {
    return shutdown;
  }

  @Override public boolean isTerminated() {
    return terminated;
  }

  @Override public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {

    asyncTracker.waitForComplete(l, timeUnit);

    return true;
  }

  @Override public void execute(Runnable runnable) {

    if (service.isShutdown()) {
      log.warn("Already shutdown");
      throw new RejectedExecutionException("Already shutdown");
    }

    // if wait=true, success will always return true, else only if there
    // is an available permit - we must still arrive for tracking purposes
    // accomplished by the phaser (AsyncTracker = Semaphore + Phaser)
    boolean success;
    try {
      success = asyncTracker.register();
    } catch (InterruptedException e) {
      log.warn("executor interrupted");
      throw new RejectedExecutionException("executor interrupted");
    }
    try {
      if (!success) {
        try {
          runnable.run();
        } finally {
          asyncTracker.arrive(false);
        }
        return;
      }
    } catch (Throwable t) {
      log.error(Class.class.getSimpleName() + " Exception", t);
      asyncTracker.arrive(false);
      throw t;
    }

    try {
      service.submit(() -> {
        Thread thread = Thread.currentThread();
        String currentName = thread.getName();
        try {
          thread.setName(name);
          runnable.run();
        } finally {
          thread.setName(currentName);
          asyncTracker.arrive();
        }
      });

    } catch (Throwable t) {
      log.error(Class.class.getSimpleName() + " Exception", t);
      asyncTracker.arrive();
      throw t;
    }
  }
}
