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

import java.util.HashSet;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.container.grouper.task.TaskNameGrouper;
import org.apache.samza.container.grouper.task.TaskNameGrouperFactory;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;

public class MockTaskNameGrouperFactory implements TaskNameGrouperFactory {
  @Override
  public TaskNameGrouper build(Config config) {
    return new MockTaskNameGrouper(config);
  }
}

class MockTaskNameGrouper implements TaskNameGrouper {
  private final Config config;

  public MockTaskNameGrouper(Config config) {
    this.config = config;
  }

  @Override
  public Set<ContainerModel> group(Set<TaskModel> taskModels) {
    return new HashSet<>(new JobInfo(config).getJobModel().getContainers().values());
  }
}
