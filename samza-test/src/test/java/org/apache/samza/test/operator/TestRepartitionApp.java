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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.test.framework.StreamApplicationIntegrationTestHarness;
import org.apache.samza.test.operator.data.PageView;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.apache.samza.test.operator.RepartitionApp.INPUT_TOPIC;

/**
 * Test driver for {@link RepartitionWindowApp}.
 */
public class TestRepartitionApp extends StreamApplicationIntegrationTestHarness {
  private static final String APP_NAME = "PageViewRepartitionApp";

  @Test
  public void testRepartitionApp() throws Exception {

    // create topics
    createTopic(INPUT_TOPIC, 3);

    // produce messages to different partitions.
    ObjectMapper mapper = new ObjectMapper();
    PageView pv = new PageView("india", "5.com", "userId1");
    produceMessage(INPUT_TOPIC, 0, "userId1", mapper.writeValueAsString(pv));
    pv = new PageView("china", "4.com", "userId2");
    produceMessage(INPUT_TOPIC, 1, "userId2", mapper.writeValueAsString(pv));
    pv = new PageView("india", "1.com", "userId1");
    produceMessage(INPUT_TOPIC, 2, "userId1", mapper.writeValueAsString(pv));
    pv = new PageView("india", "2.com", "userId1");
    produceMessage(INPUT_TOPIC, 0, "userId1", mapper.writeValueAsString(pv));
    pv = new PageView("india", "3.com", "userId1");
    produceMessage(INPUT_TOPIC, 1, "userId1", mapper.writeValueAsString(pv));

    Map<String, String> configs = new HashMap<>();
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.test.operator.P2PJobCoordinatorFactory");
    configs.put(JobConfig.PROCESSOR_ID(), "0");
    configs.put(TaskConfig.GROUPER_FACTORY(), "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");
    configs.put("streams.PageViewRepartitionApp-1-partition_by-p1.samza.system", "p2p");
    configs.put("systems.p2p.samza.factory", "org.apache.samza.system.p2p.P2PSystemFactory");

    // run the application
    runApplication(new RepartitionApp(), APP_NAME, configs);
    Thread.sleep(30000);
  }
}
