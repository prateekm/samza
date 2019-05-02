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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.checkpoint.FileCheckpointWatcherFactory;
import org.apache.samza.system.p2p.jobinfo.FixedConsumerLocalityManager;
import org.apache.samza.system.p2p.jobinfo.JobInfo;
import org.apache.samza.system.p2p.pq.RocksDBPersistentQueueFactory;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

public class Container {
  static {
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
    System.setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, "hh:mm:ss:SSS");
    System.setProperty(SimpleLogger.SHOW_THREAD_NAME_KEY, "false");
    System.setProperty(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, "true");
    System.setProperty(SimpleLogger.LEVEL_IN_BRACKETS_KEY, "true");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);
  private final int containerId;
  private final P2PSystemProducer producer;
  private final P2PSystemConsumer consumer;
  private final Set<SystemStreamPartition> p2pSSPs;
  private final Map<Integer, SourceTask> sourceTasks;
  private final Map<Integer, SinkTask> sinkTasks;

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
        LOGGER.error("Uncaught exception in Thread: {}. Prematurely exiting process.", t.getName(), e);
        System.exit(1);
      });
    int containerId = Integer.valueOf(args[0]); // == taskId == producerId == consumerId
    Map<String, String> configMap = new HashMap<>();
    configMap.put(JobConfig.JOB_CONTAINER_COUNT(), String.valueOf(Constants.Test.NUM_CONTAINERS));
    configMap.put(Constants.P2P_INPUT_NUM_PARTITIONS_CONFIG_KEY, String.valueOf(Constants.Test.NUM_PARTITIONS));
    configMap.put(TaskConfig.INPUT_STREAMS(), "input.input,p2p.output");
    MapConfig config = new MapConfig(configMap);
    MetricsRegistry metricsRegistry = new NoOpMetricsRegistry();

    JobInfo jobInfo = new JobInfo(config);

    P2PSystemProducer producer = new P2PSystemProducer(Constants.P2P_SYSTEM_NAME, containerId, new RocksDBPersistentQueueFactory(),
        new FileCheckpointWatcherFactory(), new FixedConsumerLocalityManager(), config, metricsRegistry, jobInfo);
    P2PSystemConsumer consumer = new P2PSystemConsumer(containerId, config, new NoOpMetricsRegistry(),
        System::currentTimeMillis, new FixedConsumerLocalityManager());
    Container container = new Container(containerId, config, producer, consumer, jobInfo);
    container.start();
  }

  private Container(int containerId, Config config, P2PSystemProducer producer, P2PSystemConsumer consumer, JobInfo jobInfo) {
    this.containerId = containerId;
    this.producer = producer;
    this.consumer = consumer;
    this.p2pSSPs = jobInfo.getP2PSSPsFor(containerId);
    this.sourceTasks = new HashMap<>();
    this.sinkTasks = new HashMap<>();

    jobInfo.getTasksFor(containerId).forEach(taskName -> {
        String taskNameStr = taskName.getTaskName();
        String[] taskNameParts = taskNameStr.split("\\s");
        String partitionId = taskNameParts[1];
        if (taskNameParts[0].startsWith("Source")) {
          sourceTasks.put(Integer.valueOf(partitionId), new SourceTask(taskNameStr, config, producer, jobInfo));
        } else {
          sinkTasks.put(Integer.valueOf(partitionId), new SinkTask(taskNameStr, jobInfo));
        }
      });
  }

  void start() {
    LOGGER.info("Starting Container {}.", containerId);
    producer.start();
    sourceTasks.forEach((partition, task) -> task.start());
    sinkTasks.forEach((partition, task) -> task.start());
    p2pSSPs.forEach(ssp -> consumer.register(ssp, ""));

    Thread pollThread = new Thread(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            Map<SystemStreamPartition, List<IncomingMessageEnvelope>> pollResults = consumer.poll(p2pSSPs, 1000);
            pollResults.forEach((ssp, imes) -> {
                int partitionId = ssp.getPartition().getPartitionId();
                SinkTask task = sinkTasks.get(partitionId);
                LOGGER.trace("Found SinkTask: {} for partitionId: {}", task, partitionId);
                task.process(imes);
              });
          }
        } catch (InterruptedException e) { }
      }, "ContainerPollThread");
    pollThread.start();

    consumer.start(); // blocks
  }

  void stop() {
    consumer.stop();
    producer.stop();
    sourceTasks.forEach((partition, task) -> task.stop());
    sinkTasks.forEach((partition, task) -> task.start());
  }
}
