package org.apache.solr.common.util;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class SolrInternalHttpClient extends HttpClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public SolrInternalHttpClient(HttpClientTransport transport, SslContextFactory sslContextFactory) {
    super(transport, sslContextFactory);
    assert ObjectReleaseTracker.getInstance().track(this);
  }

  @Override
  protected void doStop() throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("Stopping {}", this.getClass().getSimpleName());
    }
    try {
      super.doStop();
    } finally {
      assert ObjectReleaseTracker.getInstance().release(this);
    }
  }

}
