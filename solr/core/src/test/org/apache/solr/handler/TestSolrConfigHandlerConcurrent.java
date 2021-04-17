

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
package org.apache.solr.handler;

import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.cloud.SolrCloudBridgeTestCase;
import org.apache.solr.common.LinkedHashMapWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.RestTestHarness;
import org.junit.Ignore;
import org.junit.Test;
import org.noggit.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;


@Ignore
public class TestSolrConfigHandlerConcurrent extends SolrCloudBridgeTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void test() throws Exception {
    Map editable_prop_map = (Map) Utils.fromJSONResource("EditableSolrConfigAttributes.json");
    Map caches = (Map) editable_prop_map.get("query");

    setupRestTestHarnesses();
    List<Callable<Object>> threads = new ArrayList<>(caches.size());
    final List<List> collectErrors = new ArrayList<>();

    for (Object o : caches.entrySet()) {
      final Map.Entry e = (Map.Entry) o;
      if (e.getValue() instanceof Map) {
        List<String> errs = new ArrayList<>();
        collectErrors.add(errs);
        Map value = (Map) e.getValue();
        Callable t = new Callable() {
          @Override
          public Object call() {
            try {
              Thread.sleep(SolrTestCase.random().nextInt(TEST_NIGHTLY ? 1000 : 50));
              invokeBulkCall((String) e.getKey(), errs, value);
            } catch (Exception e1) {
              ParWork.propagateInterrupt(e1);
              return null;
            }
            return null;
          }
        };
        threads.add(t);
      }
    }


    getTestExecutor().invokeAll(threads);

    boolean success = true;

    for (List e : collectErrors) {
      if (!e.isEmpty()) {
        success = false;
        log.error("{}", e);
      }

    }

    assertTrue(collectErrors.toString(), success);


  }


  private void invokeBulkCall(String  cacheName, List<String> errs, Map val) throws Exception {

    String payload = "{" +
        "'set-property' : {'query.CACHENAME.size':'CACHEVAL1'," +
        "                  'query.CACHENAME.initialSize':'CACHEVAL2'}," +
        "'set-property': {'query.CACHENAME.autowarmCount' : 'CACHEVAL3'}" +
        "}";

    Set<String> errmessages = new HashSet<>();
    for(int i =1;i<2;i++){//make it  ahigher number
      RestTestHarness publisher = randomRestTestHarness(SolrTestCase.random());
      String response;
      String val1;
      String val2;
      String val3;
      try {
        payload = payload.replaceAll("CACHENAME" , cacheName);
        val1 = String.valueOf(10 * i + 1);
        payload = payload.replace("CACHEVAL1", val1);
        val2 = String.valueOf(10 * i + 2);
        payload = payload.replace("CACHEVAL2", val2);
        val3 = String.valueOf(10 * i + 3);
        payload = payload.replace("CACHEVAL3", val3);
  
        response = publisher.post("/config", SolrTestCaseJ4.json(payload));
      } finally {
        publisher.close();
      }
      
      Map map = (Map) Utils.fromJSONString(response);
      Object errors = map.get("errors");
      if(errors!= null){
        errs.add(new String(Utils.toJSON(errors), StandardCharsets.UTF_8));
        return;
      }

      DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection("collection1");
      List<String> urls = new ArrayList<>();
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas())
          urls.add(""+replica.getBaseUrl());
      }


      //get another node
      String url = urls.get(urls.size() - 1);

      long startTime = System.nanoTime();
      long maxTimeoutSeconds = 5;
      while ( TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutSeconds) {
        Thread.sleep(10);
        errmessages.clear();
        MapWriter m = null;
        MapWriter respMap = getAsMap(url + "/config/overlay", cloudClient);
        if (respMap != null) {
           m = (MapWriter) respMap._get("overlay/props", null);
        }

        if (respMap == null || m == null) {
            errmessages.add(StrUtils.formatString("overlay does not exist for cache: {0} , iteration: {1} response {2} ", cacheName, i, respMap));
            continue;
        }

        Object o = m._get(asList("query", cacheName, "size"), null);
        if(!val1.equals(o.toString())) errmessages.add(StrUtils.formatString("'size' property not set, expected = {0}, actual {1}", val1, o));

        o = m._get(asList("query", cacheName, "initialSize"), null);
        if(!val2.equals(o.toString())) errmessages.add(StrUtils.formatString("'initialSize' property not set, expected = {0}, actual {1}", val2, o));

        o = m._get(asList("query", cacheName, "autowarmCount"), null);
        if(!val3.equals(o.toString())) errmessages.add(StrUtils.formatString("'autowarmCount' property not set, expected = {0}, actual {1}", val3, o));
        if(errmessages.isEmpty()) break;
      }
      if(!errmessages.isEmpty()) {
        errs.addAll(errmessages);
        return;
      }
    }

  }

  public static LinkedHashMapWriter getAsMap(String uri, CloudHttp2SolrClient cloudClient) throws Exception {
    String response = Http2SolrClient.GET(uri, cloudClient.getHttpClient()).asString;
    try {
      return (LinkedHashMapWriter) Utils.MAPWRITEROBJBUILDER.apply(new JSONParser(new StringReader(response))).getVal();
    } catch (JSONParser.ParseException e) {
      log.error(response, e);
      throw e;
    }
  }
}
