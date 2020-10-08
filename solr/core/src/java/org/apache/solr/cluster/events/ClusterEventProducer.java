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
package org.apache.solr.cluster.events;

/**
 * Component that produces {@link ClusterEvent} instances.
 */
public interface ClusterEventProducer {

  /**
   * Register an event listener for processing the specified event types.
   * @param listener non-null listener. If the same instance of the listener is
   *                 already registered it will be ignored.
   * @param eventTypes non-empty array of event types that this listener
   *                   is being registered for. If this is null or empty then all types will be used.
   */
  void registerListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) throws Exception;

  /**
   * Unregister an event listener for all event types.
   * @param listener non-null listener.
   */
  void unregisterListener(ClusterEventListener listener);

  /**
   * Unregister an event listener for specified event types.
   * @param listener non-null listener.
   * @param eventTypes event types from which the listener will be unregistered. If this
   *                   is null or empty then all event types will be used
   */
  void unregisterListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes);

  static ClusterEventProducer NO_OP_PRODUCER = new ClusterEventProducer() {
    @Override
    public void registerListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) throws Exception {

    }

    @Override
    public void unregisterListener(ClusterEventListener listener) {

    }

    @Override
    public void unregisterListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {

    }
  };
}
