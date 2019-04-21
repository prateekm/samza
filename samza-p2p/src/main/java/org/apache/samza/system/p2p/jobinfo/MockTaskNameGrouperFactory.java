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

import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.TaskNameGrouper;
import org.apache.samza.container.grouper.task.TaskNameGrouperFactory;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;

public class MockTaskNameGrouperFactory implements TaskNameGrouperFactory {
  @Override
  public TaskNameGrouper build(Config config) {
    return new MockTaskNameGrouper();
  }
}

class MockTaskNameGrouper implements TaskNameGrouper {
  @Override
  public Set<ContainerModel> group(Set<TaskModel> taskModels) {
    Map<TaskName, TaskModel> c0TaskModels = new HashMap<>();
    c0TaskModels.put(new TaskName("Source 0"), new TaskModel(new TaskName("Source 0"), ImmutableSet.of(new SystemStreamPartition("kafka", "pageview-filter-input", new Partition(0))), new Partition(0)));
    c0TaskModels.put(new TaskName("Sink 0"), new TaskModel(new TaskName("Sink 0"), ImmutableSet.of(new SystemStreamPartition("p2p", "pageview-filter-1-partition_by-p2p", new Partition(0))), new Partition(0)));

    Map<TaskName, TaskModel> c1TaskModels = new HashMap<>();
    c1TaskModels.put(new TaskName("Source 1"), new TaskModel(new TaskName("Source 1"), ImmutableSet.of(new SystemStreamPartition("kafka", "pageview-filter-input", new Partition(1))), new Partition(1)));
    c1TaskModels.put(new TaskName("Sink 1"), new TaskModel(new TaskName("Sink 1"), ImmutableSet.of(new SystemStreamPartition("p2p", "pageview-filter-1-partition_by-p2p", new Partition(1))), new Partition(1)));

    Set<ContainerModel> containerModels = new HashSet<>();
    containerModels.add(new ContainerModel("0", c0TaskModels));
    containerModels.add(new ContainerModel("1", c1TaskModels));
    return containerModels;
  }
}
