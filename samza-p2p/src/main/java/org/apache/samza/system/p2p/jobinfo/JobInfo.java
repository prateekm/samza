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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.Util;

public interface JobInfo {
  int getNumContainers();
  int getNumPartitions();
  List<TaskName> getAllTasks();

  default List<TaskName> getTasksFor(int containerId) { // TODO used in tests only
    return Collections.emptyList();
  }

  default Set<SystemStreamPartition> getSSPsFor(int containerId) { // TODO used in tests only
    return Collections.emptySet();
  }

  default int getPartitionFor(byte[] key) {
    return Util.toPositive(Util.murmur2(key)) % getNumPartitions();
  }

  default int getConsumerFor(int partition) {
    return partition % getNumContainers(); // todo should be % num containers, not num tasks
  }
}

