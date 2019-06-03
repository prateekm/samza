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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.Constants;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobInfo.class);
  private final Config config;
  private final Map<SystemStreamPartition, TaskName> sspToTaskMapping;
  private final Map<TaskName, Integer> taskToContainerMapping;
  private final Map<Integer, List<String>> stageInputs;
  private final JobModel jobModel;

  public JobInfo(Config config) {
    this.config = config;
    this.stageInputs = getStagedInputs(config);
    this.sspToTaskMapping = new HashMap<>();
    this.taskToContainerMapping = new HashMap<>();
    this.jobModel = createJobModel(); // also populates taskToContainerMapping and sspToTaskMapping
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

    Map<TaskName, TaskModel> taskModels = new HashMap<>();
    for (Map.Entry<Integer, List<String>> stage : stageInputs.entrySet()) {
      Integer stageId = stage.getKey();
      List<String> stageInputs = stage.getValue();
      for (int partition = 0; partition < numInputPartitions; partition++) {
        TaskName taskName = new TaskName("Stage " + stageId + " Task " + partition);
        Set<SystemStreamPartition> taskSSPs = new HashSet<>();
        for (String stageInput : stageInputs) {
          String inputSystemName = stageInput.split("\\.")[0];
          String inputStreamName = stageInput.split("\\.")[1];
          SystemStreamPartition ssp = new SystemStreamPartition(inputSystemName, inputStreamName, new Partition(partition));
          taskSSPs.add(ssp);
          sspToTaskMapping.put(ssp, taskName);
        }
        taskModels.put(taskName, new TaskModel(taskName, taskSSPs, new Partition(partition)));
      }
    }

    Map<String, ContainerModel> containerModels = new HashMap<>();
    for (int containerId = 0; containerId < numContainers; containerId++) {
      final int finalContainerId = containerId;
      Map<TaskName, TaskModel> containerTaskModels = taskModels.entrySet().stream().filter(e -> {
        Integer taskPartitionNumber = Integer.valueOf(e.getKey().getTaskName().split("\\s")[3]);
        return taskPartitionNumber / numContainers == finalContainerId;
      }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      containerTaskModels.forEach(((taskName, taskModel) -> taskToContainerMapping.put(taskName, finalContainerId)));
      containerModels.put(String.valueOf(containerId), new ContainerModel(String.valueOf(containerId), containerTaskModels));
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

  public int getConsumerFor(SystemStreamPartition ssp) {
    return taskToContainerMapping.get(sspToTaskMapping.get(ssp));
  }

  private Map<Integer, List<String>> getStagedInputs(Config config) {
    String content = config.get(Constants.P2P_INPUT_STAGES_CONFIG_KEY);
    try {
      Map<Integer, List<String>> stageInputs = new HashMap<>();
      Map<String, Integer> inputStages = new ObjectMapper().readValue(content, new TypeReference<Map<String, Integer>>(){});
      LOGGER.info("INPUT STAGES: {}", inputStages.toString());

      for (Map.Entry<String, Integer> inputStage : inputStages.entrySet()) {
        String currentInput = inputStage.getKey();
        Integer currentStage = inputStage.getValue();
        stageInputs.compute(currentStage, (stageId, inputs) -> {
          if (inputs != null) {
            inputs.add(currentInput);
            return inputs;
          } else {
            ArrayList<String> list = new ArrayList<>();
            list.add(currentInput);
            return list;
          }
        });
      }
      LOGGER.info("STAGE INPUTS: {}", stageInputs.toString());
      return stageInputs;
    } catch (IOException e) {
      throw new SamzaException("Could not deserialize input stage map from config: " + content);
    }
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

