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
package org.apache.solr.core;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.index.LogDocMergePolicyFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestNRTOpen extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTestNRTOpen() throws Exception {
    // set these so that merges won't break the test
    System.setProperty("solr.tests.maxBufferedDocs", "100000");
    systemSetPropertySolrTestsMergePolicyFactory(LogDocMergePolicyFactory.class.getName());
    initCore("solrconfig-basic.xml", "schema-minimal.xml");

    // add a doc
    assertU(adoc("foo", "bar"));
    assertU(commit());
    // reload the core again over the same index

    h.reload();
    assertNRT(1);
  }

  @AfterClass
  public static void afterTestNRTOpen() throws Exception {
    // ensure we clean up after ourselves, this will fire before superclass...
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.tests.maxBufferedDocs");
    deleteCore();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    // delete all, then add initial doc
    assertU(delQ("*:*"));
    assertU(adoc("foo", "bar"));
    assertU(commit());
  }

  public void testReaderIsNRT() throws Exception {
    // core reload
    String core = h.coreName;

    h.getCoreContainer().reload(core);
    assertNRT(1);

    // add a doc and soft commit
    assertU(adoc("baz", "doc"));
    assertU(commit("softCommit", "true"));
    assertNRT(2);

    // add a doc and hard commit
    assertU(adoc("bazz", "doc"));
    assertU(commit());
    assertNRT(3);

    // add a doc and core reload
    assertU(adoc("bazzz", "doc2"));
    h.getCoreContainer().reload(core);
    assertNRT(4);
  }

  public void testSharedCores() {
    // clear out any junk
    assertU(optimize());

    Set<Object> s1 = getCoreCacheKeys();
    assertEquals(1, s1.size());

    // add a doc, will go in a new segment
    assertU(adoc("baz", "doc"));
    assertU(commit("softCommit", "true"));

    Set<Object> s2 = getCoreCacheKeys();
    assertEquals(2, s2.size());
    assertTrue(s2.containsAll(s1));

    // add two docs, will go in a new segment
    assertU(adoc("foo", "doc"));
    assertU(adoc("foo2", "doc"));
    assertU(commit());

    Set<Object> s3 = getCoreCacheKeys();
    assertEquals(3, s3.size());
    assertTrue(s3.containsAll(s2));

    // delete a doc
    assertU(delQ("foo2:doc"));
    assertU(commit());

    // same cores
    assertEquals(s3, getCoreCacheKeys());
  }

  static void assertNRT(int maxDoc) throws IOException {
    try (SolrCore core = h.getCore()) {
      core.withSearcher(searcher -> {
        DirectoryReader ir = searcher.getRawReader();
        assertEquals(maxDoc, ir.maxDoc());
        assertTrue("expected NRT reader, got: " + ir, ir.toString().contains(":nrt"));
        return null;
      });
    }
  }

  private Set<Object> getCoreCacheKeys() {
    try (SolrCore core = h.getCore()) {
      return core.withSearcher(searcher -> {
        Set<Object> set = Collections.newSetFromMap(new IdentityHashMap<>());
        DirectoryReader ir = searcher.getRawReader();

        for (LeafReaderContext context : ir.leaves()) {
          LeafReader reader = context.reader();
          set.add(reader.getCoreCacheHelper().getKey());

        }
        return set;
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
