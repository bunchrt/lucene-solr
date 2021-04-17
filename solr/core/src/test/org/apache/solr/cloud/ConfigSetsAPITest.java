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
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ConfigSetsAPITest extends SolrCloudTestCase {

  @BeforeClass
  public static void beforeConfigSetsAPITest() throws Exception {
    System.setProperty("shareSchema", "true");  // see testSharedSchema

    configureCluster(1) // some tests here assume 1 node
        .addConfig("conf1", SolrTestUtil.TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .addConfig("cShare", SolrTestUtil.TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .formatZk(true).configure();
  }

  @After
  public void tearDown() throws Exception {
    cluster.deleteAllCollections();
    super.tearDown();
  }

  @AfterClass
  public static void afterConfigSetsAPITest() {
    System.clearProperty("shareSchema");
  }

  @Test
  public void testConfigSetDeleteWhenInUse() throws Exception {
    CollectionAdminRequest.createCollection("test_configset_delete", "conf1", 1, 1)
        .process(cluster.getSolrClient());

    // TODO - check exception response!
    ConfigSetAdminRequest.Delete deleteConfigRequest = new ConfigSetAdminRequest.Delete();
    deleteConfigRequest.setConfigSetName("conf1");
    LuceneTestCase.expectThrows(SolrException.class, () -> {
      deleteConfigRequest.process(cluster.getSolrClient());
    });
  }

  @Test
 // @LuceneTestCase.Nightly // TODO speedup
  @Ignore // MRM
  public void testSharedSchema() throws Exception {
    CollectionAdminRequest.createCollection("col1", "cShare", 1, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    CollectionAdminRequest.createCollection("col2", "cShare", 1, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    CollectionAdminRequest.createCollection("col3", "conf1", 1, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);

    CoreContainer coreContainer = cluster.getJettySolrRunner(0).getCoreContainer();

    try (SolrCore coreCol1 = coreContainer.getCore("col1_s1_r_n1");
         SolrCore coreCol2 = coreContainer.getCore("col2_s1_r_n1");
         SolrCore coreCol3 = coreContainer.getCore("col3_s1_r_n1")) {
      assertSame(coreContainer.getAllCoreNames().toString(), coreCol1.getLatestSchema(), coreCol2.getLatestSchema());
      assertNotSame(coreCol1.getLatestSchema(), coreCol3.getLatestSchema());
    }

    // change col1's configSet
    CollectionAdminRequest.modifyCollection("col1",
        SolrTestCaseJ4.map("collection.configName", "conf1")  // from cShare
    ).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);

    try (SolrCore coreCol1 = coreContainer.getCore("col1_s1_r_n1");
         SolrCore coreCol2 = coreContainer.getCore("col2_s1_r_n1")) {
      assertNotSame(coreCol1.getLatestSchema(), coreCol2.getLatestSchema());
    }

  }

}
