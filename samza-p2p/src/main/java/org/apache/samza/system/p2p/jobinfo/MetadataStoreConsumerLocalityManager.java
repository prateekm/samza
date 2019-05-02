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

import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.coordinator.stream.messages.SetP2PConsumerPortMapping;
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataStoreConsumerLocalityManager implements ConsumerLocalityManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStoreConsumerLocalityManager.class);

  private final CoordinatorStreamStore coordinatorStreamStore;
  private final LocalityManager localityManager;
  private final P2PPortManager portManager;

  public MetadataStoreConsumerLocalityManager(Config config, MetricsRegistry metrics) {
    this.coordinatorStreamStore = new CoordinatorStreamStore(config, metrics);
    this.localityManager = new LocalityManager(new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetContainerHostMapping.TYPE));
    this.portManager = new P2PPortManager(new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetP2PConsumerPortMapping.TYPE));
  }

  @Override
  public void start() {
    coordinatorStreamStore.init();
  }

  @Override
  public InetSocketAddress getConsumerAddress(String containerId) {
      String consumerHost = null;
      long consumerPort = -1;
      while (consumerHost == null || consumerPort == -1) { // TODO is not atomic, maybe retry will fix
        Map<String, String> localityMapping = localityManager.readContainerLocality().get(String.valueOf(containerId));
        if (localityMapping != null && !localityMapping.isEmpty()) {
          consumerHost = localityMapping.get(SetContainerHostMapping.HOST_KEY);
        } else {
          LOGGER.info("Waiting for host to become available for Consumer: {}", containerId);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) { }
          continue; // no point waiting for port, retry
        }

        String port = portManager.readConsumerPorts().get(containerId);
        if (StringUtils.isNotBlank(port)) {
          consumerPort = Long.valueOf(port);
        } else {
          LOGGER.info("Waiting for port to become available for Consumer: {}", containerId);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) { }
          continue;
        }
      }

      return new InetSocketAddress(consumerHost, (int) consumerPort);
    }

  @Override
  public void writeConsumerPort(String containerId, Integer port) {
    portManager.writeConsumerPort(containerId, port);
  }

  @Override
  public void stop() {
    coordinatorStreamStore.close();
  }
}
