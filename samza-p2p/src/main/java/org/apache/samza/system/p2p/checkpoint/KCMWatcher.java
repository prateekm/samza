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
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.Constants;
import org.apache.samza.system.p2p.ProducerOffset;
import org.apache.samza.system.p2p.Util;
import org.apache.samza.system.p2p.jobinfo.JobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KCMWatcher implements CheckpointWatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(KCMWatcher.class);

  private volatile boolean shutdown;
  private Thread watcherThread;
  private CheckpointManager checkpointManager;
  private List<TaskName> allTasks;

  KCMWatcher(CheckpointManager checkpointManager, List<TaskName> allTasks) {
    this.checkpointManager = checkpointManager;
    this.allTasks = allTasks;
  }

  public void updatePeriodically(String systemName, int producerId, JobInfo jobInfo,
      ConcurrentMap<SystemStreamPartition, ProducerOffset> lastTaskCheckpointedOffsets) {
    allTasks.forEach(checkpointManager::register);
    checkpointManager.start();
    this.watcherThread = new Thread(() -> {
        while (!shutdown && !Thread.currentThread().isInterrupted()) {
          allTasks.stream()
              .filter(tn -> tn.getTaskName().contains("Sink")) // TODO only updating sink checkpoints for now
              .map(taskName -> {
                  Checkpoint checkpoint = checkpointManager.readLastCheckpoint(taskName);
                  LOGGER.trace("Read task: {} checkpoint: {}", taskName, checkpoint);
                  return Pair.of(taskName, checkpoint);
                })
              .filter(cp -> cp.getRight() != null && !cp.getRight().getOffsets().isEmpty())
              .flatMap(cp -> {
                    return cp.getRight().getOffsets().entrySet().stream() // find (ssp, offset) on the p2p system
                        .filter(e -> e.getKey().getSystemStream().getSystem().equals(systemName))
                        .map(e -> Pair.of(e.getKey(), e.getValue())); // (taskname, p2p ssp offset)
                  })
              .forEach(p -> {
                    ProducerOffset offset = new ProducerOffset(Util.parseOffsetVector(p.getRight())[producerId]);
                    LOGGER.trace("Setting p2p ssp: {} checkpointed offset for producer: {} to: {}",
                        p.getLeft(), producerId, offset);
                    // TODO handle broadcast p2p streams? same ssp in multiple tasks
                    lastTaskCheckpointedOffsets.put(p.getLeft(), offset);
                  });
          lastTaskCheckpointedOffsets.put(Constants.CHECKPOINTS_READ_ONCE_DUMMY_KEY, ProducerOffset.MIN_VALUE);

          try {
            Thread.sleep(Constants.PRODUCER_CHECKPOINT_WATCHER_INTERVAL);
          } catch (InterruptedException e) {
            // ignore
          }
        }
      }, "KCMWatcher");
    watcherThread.start();
  }

  public void close() {
    this.shutdown = true;
    this.watcherThread.interrupt();
    checkpointManager.stop();
  }
}
