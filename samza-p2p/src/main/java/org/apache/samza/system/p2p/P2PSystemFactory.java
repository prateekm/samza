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
package org.apache.samza.system.p2p;

import org.apache.samza.config.Config;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.p2p.checkpoint.KCMWatcherFactory;
import org.apache.samza.system.p2p.jobinfo.JobInfo;
import org.apache.samza.system.p2p.pq.RocksDBPersistentQueueFactory;

public class P2PSystemFactory implements SystemFactory {
  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    String containerId = System.getenv(ShellCommandConfig.ENV_CONTAINER_ID()); // TODO only works in YARN
    return new P2PSystemConsumer(Integer.valueOf(containerId), config, registry, System::currentTimeMillis);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    String containerId = System.getenv(ShellCommandConfig.ENV_CONTAINER_ID()); // TODO only works in YARN
    return new P2PSystemProducer(systemName, Integer.valueOf(containerId), new RocksDBPersistentQueueFactory(), new KCMWatcherFactory(), config, registry, new JobInfo(config));
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new P2PSystemAdmin(config);
  }
}
