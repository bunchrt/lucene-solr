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
package org.apache.solr.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SolrMetricRegistry extends MetricRegistry {

  private ConcurrentHashMap<String,Metric> map;

  protected ConcurrentMap<String,Metric> buildMap() {
    // some hold as many 500+
    this.map = new ConcurrentHashMap<>(64);
    return this.map;
  }

  public void registerAll(String prefix, MetricSet metrics) throws IllegalArgumentException {
    metrics.getMetrics().forEach((s, metric) -> {
      if (metric instanceof MetricSet) {
        registerAll(name(prefix, s), (MetricSet) metric);
      } else {
        register(name(prefix, s), metric);
      }
    });
  }

  public void removeMatching(MetricFilter filter) {
    map.entrySet().removeIf(stringMetricEntry -> filter.matches(stringMetricEntry.getKey(), stringMetricEntry.getValue()));
  }

  public ConcurrentHashMap<String,Metric> getMap() {
    return map;
  }

  public Metric register(String fullName, Metric value) {
    register(fullName, value, true);
    return value;
  }

  public void register(String fullName, Metric value, boolean force) {
    if (force) {
      map.put(fullName, value);
    } else {
      map.putIfAbsent(fullName, value);
    }
  }

  public void clear() {
    map.clear();
  }
}
