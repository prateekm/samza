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

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.p2p.jobinfo.JobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.system.p2p.Constants.P2P_SYSTEM_NAME;

public class SourceTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SourceTask.class);
  private static final Random RANDOM = new Random();

  private final SystemStream p2pSystemStream;
  private final String taskName;
  private final JobInfo jobInfo;
  private final Thread produceThread;
  private final Thread commitThread;

  private volatile boolean shutdown = false;

  SourceTask(String taskName, Config config, P2PSystemProducer producer, JobInfo jobInfo) {
    this.taskName = taskName;
    this.jobInfo = jobInfo;
    String p2pStream = config.get(TaskConfig.INPUT_STREAMS()).split(",")[1].split("\\.")[1];
    this.p2pSystemStream = new SystemStream(P2P_SYSTEM_NAME, p2pStream);
    this.produceThread = new Thread(() -> {
        while (!shutdown && !Thread.currentThread().isInterrupted()) {
          int keyLength = 4;
          int valueLength = RANDOM.nextInt(Constants.Test.TASK_MAX_KEY_VALUE_LENGTH);
          byte[] key = new byte[keyLength];
          byte[] value = new byte[valueLength];
          ByteBuffer.wrap(key).putInt(Math.abs(RANDOM.nextInt()));
          RANDOM.nextBytes(value);
          producer.send(taskName, new OutgoingMessageEnvelope(p2pSystemStream, key, value));

          try {
            Thread.sleep(Constants.Test.TASK_PRODUCE_INTERVAL);
          } catch (InterruptedException e) { }
        }
      }, "TaskProduceThread " + taskName);

    this.commitThread = new Thread(() -> {
        while (!shutdown && !Thread.currentThread().isInterrupted()) {
          LOGGER.info("Flushing producer for task: {}.", taskName);
          long startTime = System.currentTimeMillis();
          producer.flush(taskName);
          LOGGER.info("Took {} ms to flush for task {}", System.currentTimeMillis() - startTime, taskName);

          try {
            Thread.sleep(Constants.Test.TASK_FLUSH_INTERVAL);
          } catch (InterruptedException e) { }
        }
      }, "TaskCommitThread " + taskName);
  }

  void start() {
    this.produceThread.start();
    this.commitThread.start();
  }

  void stop() {
    this.shutdown = true;
    this.produceThread.interrupt();
    this.commitThread.interrupt();
  }
}
