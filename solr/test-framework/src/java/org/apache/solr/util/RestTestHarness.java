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
package org.apache.solr.util;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.ObjectReleaseTrackerTestImpl;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Facilitates testing Solr's REST API via a provided embedded Jetty
 */
public class RestTestHarness extends BaseTestHarness implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private RESTfulServerProvider serverProvider;
  private Http2SolrClient sorlClient;
  
  public RestTestHarness(RESTfulServerProvider serverProvider, Http2SolrClient sorlClient, SolrResourceLoader loader) {
    super(loader);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, 5000);
    params.set(HttpClientUtil.PROP_SO_TIMEOUT, 10000);
    this.sorlClient = sorlClient;
    this.serverProvider = serverProvider;
    assert ObjectReleaseTracker.getInstance().track(this);
  }
  
  public String getBaseURL() {
    return serverProvider.getBaseURL();
  }

  public void setServerProvider(RESTfulServerProvider serverProvider) {
    this.serverProvider = serverProvider;
  }

  public RESTfulServerProvider getServerProvider() {
    return this.serverProvider;
  }

  public String getAdminURL() {
    return getBaseURL().replace("/collection1", "");
  }
  
  /**
   * Validates an XML "query" response against an array of XPath test strings
   *
   * @param request the Query to process
   * @return null if all good, otherwise the first test that fails.
   * @exception Exception any exception in the response.
   * @exception java.io.IOException if there is a problem writing the XML
   */
  public String validateQuery(String request, String... tests) throws Exception {

    String res = query(request);
    return validateXPathWithEntities(loader, res, tests);
  }


  /**
   * Validates an XML PUT response against an array of XPath test strings
   *
   * @param request the PUT request to process
   * @param content the content to send with the PUT request
   * @param tests the validating XPath tests
   * @return null if all good, otherwise the first test that fails.
   * @exception Exception any exception in the response.
   * @exception java.io.IOException if there is a problem writing the XML
   */
  public String validatePut(String request, String content, String... tests) throws Exception {

    String res = put(request, content);
    return validateXPathWithEntities(loader, res, tests);
  }


  /**
   * Processes a "query" using a URL path (with no context path) + optional query params,
   * e.g. "/schema/fields?indent=off"
   *
   * @param request the URL path and optional query params
   * @return The response to the query
   * @exception Exception any exception in the response.
   */
  public String query(String request) throws Exception {
    return Http2SolrClient.GET(getBaseURL() + request, sorlClient).asString;
  }

  public String adminQuery(String request) throws Exception {
    return Http2SolrClient.GET(getAdminURL()  + request, sorlClient).asString;
  }

  /**
   * Processes a PUT request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/fields/newfield", PUTs the given content, and returns the response content.
   * 
   * @param request The URL path and optional query params
   * @param content The content to include with the PUT request
   * @return The response to the PUT request
   */
  public String put(String request, String content) throws IOException {
    String resp;
    try {
      resp = Http2SolrClient.PUT(getBaseURL() + request, sorlClient, content.getBytes("UTF-8"), "application/json",
          Collections.emptyMap()).asString;
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (ExecutionException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (TimeoutException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    return resp;
  }

  /**
   * Processes a DELETE request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/analysis/protwords/english", and returns the response content.
   *
   * @param request the URL path and optional query params
   * @return The response to the DELETE request
   */
  public String delete(String request) throws IOException {
    try {
      return Http2SolrClient.DELETE(getBaseURL() + request).asString;
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (ExecutionException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (TimeoutException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Processes a POST request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/fields/newfield", PUTs the given content, and returns the response content.
   *
   * @param request The URL path and optional query params
   * @param content The content to include with the POST request
   * @return The response to the POST request
   */
  public String post(String request, String content) throws IOException {
    String resp = null;
    try (Http2SolrClient http2SolrClient =  new Http2SolrClient.Builder().build()) {
      resp = Http2SolrClient.POST(getBaseURL() + request, http2SolrClient, content.getBytes("UTF-8"), "application/json").asString;
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (ExecutionException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (TimeoutException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    return resp;
  }


  public String checkResponseStatus(String xml, String code) throws Exception {
    try {
      String response = query(xml);
      String valid = validateXPathWithEntities(loader, response,"//int[@name='status']="+code );
      return (null == valid) ? null : response;
    } catch (XPathExpressionException e) {
      throw new RuntimeException("?!? static xpath has bug?", e);
    }
  }

  public String checkAdminResponseStatus(String xml, String code) throws Exception {
    try {
      String response = adminQuery(xml);
      String valid = validateXPathWithEntities(loader, response,"//int[@name='status']="+code );
      return (null == valid) ? null : response;
    } catch (XPathExpressionException e) {
      throw new RuntimeException("?!? static xpath has bug?", e);
    }
  }
  /**
   * Reloads the first core listed in the response to the core admin handler STATUS command
   */
  @Override
  public void reload() throws Exception {
    String coreName = (String)evaluateXPath
        (loader, adminQuery("/admin/cores?wt=xml&action=STATUS"),
         "//lst[@name='status']/lst[1]/str[@name='name']",
         XPathConstants.STRING);

    if (coreName.length() > 0) {

      String xml = checkAdminResponseStatus("/admin/cores?wt=xml&action=RELOAD&core=" + coreName, "0");
      if (null != xml) {
        throw new RuntimeException("RELOAD failed:\n" + xml);
      }
    } else {
      log.warn("Core name came back from status call as empty string");
    }
  }

  /**
   * Processes an "update" (add, commit or optimize) and
   * returns the response as a String.
   *
   * @param xml The XML of the update
   * @return The XML response to the update
   */
  @Override
  public String update(String xml) {
    try {
      return query("/update?stream.body=" + URLEncoder.encode(xml, "UTF-8"));
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    assert ObjectReleaseTracker.getInstance().release(this);
  }
}
