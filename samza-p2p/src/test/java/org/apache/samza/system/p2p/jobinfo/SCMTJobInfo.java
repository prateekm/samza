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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.Constants;

public class SCMTJobInfo implements JobInfo {
  @Override
  public int getNumContainers() {
    return Constants.NUM_CONTAINERS;
  }

  @Override
  public int getNumPartitions() {
    return Constants.NUM_PARTITIONS;
  }

  @Override
  public List<TaskName> getAllTasks() {
    ArrayList<TaskName> taskNames = new ArrayList<>();
    for (int i = 0; i < getNumPartitions(); i++) {
      taskNames.add(new TaskName("Source Partition " + i));
      taskNames.add(new TaskName("Sink Partition " + i));
    }
    return taskNames;
  }

  @Override
  public List<TaskName> getTasksFor(int containerId) {
    return getAllTasks();
  }

  @Override
  public Set<SystemStreamPartition> getSSPsFor(int containerId) {
    Set<SystemStreamPartition> ssps = new HashSet<>();
    for (int i = 0; i < Constants.NUM_PARTITIONS; i++) {
      ssps.add(new SystemStreamPartition(Constants.SYSTEM_NAME, Constants.STREAM_NAME, new Partition(i)));
    }
    return ssps;
  }
}
