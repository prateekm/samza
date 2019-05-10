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

import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.Constants;
import org.apache.samza.system.p2p.ProducerOffset;
import org.apache.samza.system.p2p.Util;
import org.apache.samza.system.p2p.jobinfo.JobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileCheckpointWatcher implements CheckpointWatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileCheckpointWatcher.class);
  private Thread watcher;
  private volatile boolean shutdown = false;

  @Override
  public void updatePeriodically(String systemName, int producerId, JobInfo jobInfo,
      ConcurrentMap<SystemStreamPartition, ProducerOffset> lastTaskCheckpointedOffsets) {
    this.watcher = new Thread(() -> {
        while (!shutdown && !Thread.currentThread().isInterrupted()) {
          try {
            List<TaskName> tasks = jobInfo.getAllTasks();
            for (TaskName taskName : tasks) {
              if (taskName.getTaskName().startsWith("Source")) continue; // only check checkpoints for sinks

              String fileContents = Util.readFileString(Constants.Test.getTaskCheckpointPath(taskName.getTaskName()));
              String[] fileParts = fileContents.split(":");
              String sspString = fileParts[0];
              String offsets = fileParts[1];
              String[] sspParts = sspString.substring("SystemStreamPartition [".length(), sspString.length()-1).split(",");
              SystemStreamPartition ssp = new SystemStreamPartition(sspParts[0].trim(), sspParts[1].trim(),
                  new Partition(Integer.valueOf(sspParts[2].trim())));

              String producerOffset = Util.parseOffsetVector(offsets)[producerId].trim();
              LOGGER.trace("Setting checkpointed offset for ssp: {} to: {}", ssp, producerOffset);
              // TODO BLOCKER FIX CHECKPOINT FORMAT TO SUPPORT MULTI P2P SSP PER TASK
              lastTaskCheckpointedOffsets.put(ssp, new ProducerOffset(producerOffset));
            }
            lastTaskCheckpointedOffsets.put(Constants.CHECKPOINTS_READ_ONCE_DUMMY_KEY, ProducerOffset.MIN_VALUE);
          } catch (NoSuchFileException e) {
            LOGGER.info("No checkpoint file found. Assuming first deploy.");
            lastTaskCheckpointedOffsets.put(Constants.CHECKPOINTS_READ_ONCE_DUMMY_KEY, ProducerOffset.MIN_VALUE);
          } catch (Exception e) {
            LOGGER.error("Error finding last checkpointed offsets for producerId: {}.", producerId, e);
          }

          try {
            Thread.sleep(Constants.PRODUCER_CHECKPOINT_WATCHER_INTERVAL);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }, "FileCheckpointWatcher " + producerId);
    watcher.start();
  }

  @Override
  public void close() {
    this.shutdown = true;
    this.watcher.interrupt();
  }
}
