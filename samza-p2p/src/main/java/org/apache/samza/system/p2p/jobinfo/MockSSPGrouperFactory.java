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
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory;
import org.apache.samza.system.SystemStreamPartition;

public class MockSSPGrouperFactory implements SystemStreamPartitionGrouperFactory {

  @Override
  public SystemStreamPartitionGrouper getSystemStreamPartitionGrouper(Config config) {
    return new MockSSPGrouper();
  }
}

class MockSSPGrouper implements SystemStreamPartitionGrouper {
  @Override
  public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> systemStreamPartitions) {
    Map<TaskName, Set<SystemStreamPartition>> taskModels = new HashMap<>();
    taskModels.put(new TaskName("Source 0"), ImmutableSet.of(new SystemStreamPartition("kafka", "pageview-filter-input", new Partition(0))));
    taskModels.put(new TaskName("Sink 0"), ImmutableSet.of(new SystemStreamPartition("p2p", "pageview-filter-1-partition_by-p2p", new Partition(0))));

    taskModels.put(new TaskName("Source 1"), ImmutableSet.of(new SystemStreamPartition("kafka", "pageview-filter-input", new Partition(1))));
    taskModels.put(new TaskName("Sink 1"), ImmutableSet.of(new SystemStreamPartition("p2p", "pageview-filter-1-partition_by-p2p", new Partition(1))));
    return taskModels;
  }
}
