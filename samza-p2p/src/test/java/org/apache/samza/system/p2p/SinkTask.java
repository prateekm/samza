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

import com.google.common.base.Preconditions;

import java.util.List;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.p2p.jobinfo.JobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SourceTask.class);
  private final String taskName;
  private final JobInfo jobInfo;
  private final Thread commitThread;

  private volatile String lastReceivedOffset = null;
  private volatile boolean shutdown = false;

  public SinkTask(String taskName, JobInfo jobInfo) {
    this.taskName = taskName;
    this.jobInfo = jobInfo;

    this.commitThread = new Thread(() -> {
        while (!shutdown && !Thread.currentThread().isInterrupted()) {
          String currentLastReceivedOffset = this.lastReceivedOffset;
          if (currentLastReceivedOffset != null) {
            LOGGER.info("Writing checkpoint file with offset: {}", currentLastReceivedOffset);
            try {
              Util.writeFile(Constants.Test.getTaskCheckpointPath(taskName), currentLastReceivedOffset);
            } catch (Exception e) {
              throw new SamzaException("Could not write checkpoint file.", e);
            }
          }

          try {
            Thread.sleep(Constants.Test.TASK_FLUSH_INTERVAL);
          } catch (InterruptedException e) { }
        }
      }, "TaskCommitThread " + taskName);
  }

  void start() {
    this.commitThread.start();
  }

  void process(List<IncomingMessageEnvelope> imes) {
    imes.forEach(ime -> {
        LOGGER.trace("Processing polled message with offset: {} in task: {}", ime.getOffset(), taskName);
        int partition = jobInfo.getPartitionFor((byte[]) ime.getKey());
        Preconditions.checkState(("Sink " + partition).equals(taskName));
        // TODO record data / add more asserts
      });

    this.lastReceivedOffset = imes.get(imes.size() - 1).getOffset();
  }

  void stop() {
    this.shutdown = true;
    this.commitThread.interrupt();
  }
}
