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

import com.google.common.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.Constants;

public class JobInfo {
  private final Config config;
  private final Map<Integer, Integer> taskToContainerMapping;
  private final Map<Integer, Integer> p2pPartitionToTaskMapping;
  private final JobModel jobModel;

  public JobInfo(Config config) {
    this.config = config;
    this.taskToContainerMapping = new HashMap<>();
    this.p2pPartitionToTaskMapping = new HashMap<>();
    this.jobModel = createJobModel(); // also populates taskToContainerMapping and p2pPartitionToTaskMapping
  }

  public int getNumPartitions() {
    return config.getInt(Constants.P2P_INPUT_NUM_PARTITIONS_CONFIG_KEY); // TODO assumes num p2p partitions = num source partitions
  }

  public JobModel getJobModel() {
    return jobModel;
  }

  private JobModel createJobModel() {
    int numContainers = config.getInt(JobConfig.JOB_CONTAINER_COUNT());
    int numInputPartitions = config.getInt(Constants.P2P_INPUT_NUM_PARTITIONS_CONFIG_KEY);

    String[] taskInputs = config.get(TaskConfig.INPUT_STREAMS()).split(",");
    List<String> p2pSystemStreams = new ArrayList<>();
    List<String> inputSystemStreams = new ArrayList<>();
    for (int i = 0; i < taskInputs.length; i++) {
      if (taskInputs[i].startsWith(Constants.P2P_SYSTEM_NAME)) {
        p2pSystemStreams.add(taskInputs[i]);
      } else {
        inputSystemStreams.add(taskInputs[i]);
      }
    }

    Map<String, ContainerModel> containerModels = new HashMap<>();
    int taskNumber = 0;
    int numSourceTasksPerContainer = numInputPartitions / numContainers;
    for (int containerId = 0; containerId < numContainers; containerId++) {
      Map<TaskName, TaskModel> taskModels = new HashMap<>();
      for (int j = 0; j < numSourceTasksPerContainer; j++) {
        Set<SystemStreamPartition> inputSSPs = new HashSet<>();
        int finalTaskNumber = taskNumber;
        inputSystemStreams.forEach(ss -> {
          String inputSystemName = ss.split("\\.")[0];
          String inputStreamName = ss.split("\\.")[1];
          inputSSPs.add(new SystemStreamPartition(inputSystemName, inputStreamName, new Partition(finalTaskNumber)));
        });
        TaskName sourceTaskName = new TaskName("Source " + taskNumber);
        taskModels.put(sourceTaskName, new TaskModel(sourceTaskName, inputSSPs, new Partition(taskNumber)));

        Set<SystemStreamPartition> p2pSSPs = new HashSet<>();
        p2pSystemStreams.forEach(ss -> {
          String p2pSystemName = ss.split("\\.")[0];
          String p2pStreamName = ss.split("\\.")[1];
          p2pSSPs.add(new SystemStreamPartition(p2pSystemName, p2pStreamName, new Partition(finalTaskNumber)));
        });
        TaskName sinkTaskName = new TaskName("Sink " + taskNumber);
        taskModels.put(sinkTaskName, new TaskModel(sinkTaskName, p2pSSPs, new Partition(taskNumber)));

        p2pPartitionToTaskMapping.put(taskNumber, taskNumber); // TODO assumes p2p partition num == task num
        taskToContainerMapping.put(taskNumber, containerId);
        taskNumber++;
      }
      containerModels.put(String.valueOf(containerId), new ContainerModel(String.valueOf(containerId), taskModels));
    }

    return new JobModel(config, containerModels);
  }

  public List<TaskName> getAllTasks() {
    List<TaskName> tasks = new ArrayList<>();
    getJobModel().getContainers().forEach((cid, cm) -> tasks.addAll(cm.getTasks().keySet()));
    return tasks;
  }

  public int getPartitionFor(byte[] key) {
    // return Util.toPositive(Util.murmur2(key)) % getNumPartitions();
    return ByteBuffer.wrap(key).getInt() % getNumPartitions(); // TODO BLOCKER REVERT
  }

  public int getConsumerFor(int partition) {
    return taskToContainerMapping.get(p2pPartitionToTaskMapping.get(partition));
  }

  public int getTaskFor(int partition) {
    return p2pPartitionToTaskMapping.get(partition);
  }

  @VisibleForTesting // used in tests only
  public List<TaskName> getTasksFor(int containerId) {
    ContainerModel containerModel = getJobModel().getContainers().get(String.valueOf(containerId));
    return new ArrayList<>(containerModel.getTasks().keySet());
  }

  @VisibleForTesting // used in tests only
  public Set<SystemStreamPartition> getP2PSSPsFor(int containerId) {
    Set<SystemStreamPartition> ssps = new HashSet<>();
    ContainerModel containerModel = getJobModel().getContainers().get(String.valueOf(containerId));
    containerModel.getTasks().values().forEach(tm -> ssps.addAll(tm.getSystemStreamPartitions()));
    return ssps.stream().filter(ssp -> ssp.getSystem().equals(Constants.P2P_SYSTEM_NAME)).collect(Collectors.toSet());
  }
}

