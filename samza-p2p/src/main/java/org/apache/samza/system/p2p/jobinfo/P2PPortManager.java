/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.system.p2p.jobinfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetP2PConsumerPortMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.serializers.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for persisting and reading the p2p consumer port assignment information into the metadata store.
 * */
public class P2PPortManager {
  private static final Logger LOG = LoggerFactory.getLogger(P2PPortManager.class);

  private final Serde<String> valueSerde;
  private final MetadataStore metadataStore;

  public P2PPortManager(MetadataStore metadataStore) {
    this.metadataStore = metadataStore;
    this.valueSerde = new CoordinatorStreamValueSerde(SetP2PConsumerPortMapping.TYPE);
  }

  /**
   * Method to allow read container p2p port information from the {@link MetadataStore}.
   *
   * @return the map of containerId: port
   */
  public Map<String, String> readConsumerPorts() {
    Map<String, String> allMappings = new HashMap<>();
    metadataStore.all().forEach((containerId, valueBytes) -> {
        if (valueBytes != null) {
          String port = valueSerde.fromBytes(valueBytes);
          allMappings.put(containerId, port);
        }
      });

    return Collections.unmodifiableMap(allMappings);
  }

  /**
   * Method to write p2p consumer port information to the {@link MetadataStore}.
   *
   * @param containerId  the container ID
   * @param port  the port
   */
  public void writeConsumerPort(String containerId, Integer port) {
    LOG.info("Writing p2p port: {} for container: {}", port, containerId);
    metadataStore.put(containerId, valueSerde.toBytes(String.valueOf(port)));
  }

  public void close() {
    metadataStore.close();
  }
}
