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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.p2p.Util;
import org.apache.samza.system.p2p.jobinfo.JobInfo;

public class KCMWatcher implements CheckpointWatcher {
  private volatile boolean shutdown;
  private Thread watcherThread;
  private CheckpointManager checkpointManager;
  private List<TaskName> allTasks;

  KCMWatcher(CheckpointManager checkpointManager, List<TaskName> allTasks) {
    this.checkpointManager = checkpointManager;
    this.allTasks = allTasks;
  }

  public void updatePeriodically(String systemName, int producerId, JobInfo jobInfo, AtomicLong minCheckpointedOffset) {
    allTasks.forEach(checkpointManager::register);
    checkpointManager.start();
    this.watcherThread = new Thread(() -> {
        while (!shutdown && !Thread.currentThread().isInterrupted()) {
          long minOffset = allTasks.stream()
              .map(checkpointManager::readLastCheckpoint)
              .map(cp -> cp.getOffsets().entrySet().stream()
                  .filter(e -> e.getKey().getSystemStream().getSystem().equals(systemName))
                  .map(Map.Entry::getValue).findFirst()
                  .get())
              .min(Comparator.comparingLong(o -> Util.parseOffsets(o)[producerId]))
              .map(Long::valueOf).get();
          minCheckpointedOffset.set(minOffset);
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            // ignore
          }
        }
      });
    watcherThread.start();
  }

  public void close() {
    this.shutdown = true;
    this.watcherThread.interrupt();
    checkpointManager.stop();
  }
}
