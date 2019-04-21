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
package org.apache.samza.test.operator;

import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.runtime.LocationIdProvider;
import org.apache.samza.runtime.LocationIdProviderFactory;
import org.apache.samza.standalone.PassthroughJobCoordinator;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class P2PJobCoordinator implements JobCoordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PassthroughJobCoordinator.class);
  private final String processorId;
  private final Config config;
  private final LocationId locationId;
  private JobCoordinatorListener coordinatorListener = null;

  public P2PJobCoordinator(String processorId, Config config, MetricsRegistry metricsRegistry) {
    this.processorId = processorId;
    this.config = config;
    LocationIdProviderFactory locationIdProviderFactory = Util.getObj(new JobConfig(config).getLocationIdProviderFactory(), LocationIdProviderFactory.class);
    LocationIdProvider locationIdProvider = locationIdProviderFactory.getLocationIdProvider(config);
    this.locationId = locationIdProvider.getLocationId();
  }

  @Override
  public void start() {
    // No-op
    JobModel jobModel = null;
    try {
      jobModel = getJobModel();
      CheckpointManager checkpointManager = new TaskConfigJava(jobModel.getConfig()).getCheckpointManager(null);
      if (checkpointManager != null) {
        checkpointManager.createResources();
      }

      ChangelogStreamManager.createChangelogStreams(config, jobModel.maxChangeLogStreamPartitions);
    } catch (Exception e) {
      LOGGER.error("Exception while trying to getJobModel.", e);
      if (coordinatorListener != null) {
        coordinatorListener.onCoordinatorFailure(e);
      }
    }
    if (jobModel != null && jobModel.getContainers().containsKey(processorId)) {
      if (coordinatorListener != null) {
        coordinatorListener.onJobModelExpired();
        coordinatorListener.onNewJobModel(processorId, jobModel);
      }
    } else {
      LOGGER.info("JobModel: {} does not contain processorId: {}. Stopping the JobCoordinator", jobModel, processorId);
      stop();
    }
  }

  @Override
  public void stop() {
    // No-op
    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
      coordinatorListener.onCoordinatorStop();
    }
  }

  @Override
  public void setListener(JobCoordinatorListener listener) {
    this.coordinatorListener = listener;
  }

  @Override
  public JobModel getJobModel() {
    Map<TaskName, TaskModel> taskModels = new HashMap<>();
    taskModels.put(new TaskName("Source 0"), new TaskModel(new TaskName("Source 0"), ImmutableSet.of(new SystemStreamPartition("kafka", "pageview-filter-input", new Partition(0))), new Partition(0)));
    taskModels.put(new TaskName("Source 1"), new TaskModel(new TaskName("Source 1"), ImmutableSet.of(new SystemStreamPartition("kafka", "pageview-filter-input", new Partition(1))), new Partition(1)));
    taskModels.put(new TaskName("Sink 0"), new TaskModel(new TaskName("Sink 0"), ImmutableSet.of(new SystemStreamPartition("p2p", "pageview-filter-1-partition_by-p2p", new Partition(0))), new Partition(0)));
    taskModels.put(new TaskName("Sink 1"), new TaskModel(new TaskName("Sink 1"), ImmutableSet.of(new SystemStreamPartition("p2p", "pageview-filter-1-partition_by-p2p", new Partition(1))), new Partition(1)));
    Map<String, ContainerModel> containerModels = new HashMap<>();
    containerModels.put("0", new ContainerModel("0", taskModels));
    return new JobModel(config, containerModels);
  }

  @Override
  public String getProcessorId() {
    return this.processorId;
  }
}
