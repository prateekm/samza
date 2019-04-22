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
package org.apache.samza.system.p2p.checkpoint;

import java.util.List;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;

public class KCMWatcherFactory implements CheckpointWatcherFactory {
  public CheckpointWatcher getCheckpointWatcher(Config config, List<TaskName> tasks, MetricsRegistry metricsRegistry) {
    try {
      CheckpointManager checkpointManager =
          ((CheckpointManagerFactory) Class.forName(config.get(TaskConfig.CHECKPOINT_MANAGER_FACTORY())).newInstance())
              .getCheckpointManager(config, metricsRegistry);

      return new KCMWatcher(checkpointManager, tasks);
    } catch (Exception e) {
      throw new SamzaException("Could not create a KCMWatcher", e);
    }
  }
}
