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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;

public class P2PSystemAdmin implements SystemAdmin {
  private final Config config;

  public P2PSystemAdmin(Config config) {
    this.config = config;
  }

  @Override
  public void start() { }

  @Override
  public void stop() { }

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    return offsets;
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    HashMap<String, SystemStreamMetadata> metadata = new HashMap<>();
    HashMap<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata = new HashMap<>();
    for (int i = 0; i < 2; i++) { // TODO fix default of 2 to be num tasks
      partitionMetadata.put(new Partition(i), new SystemStreamMetadata.SystemStreamPartitionMetadata("0", "0", "0"));
    }
    streamNames.forEach(streamName -> metadata.put(streamName, new SystemStreamMetadata(streamName, partitionMetadata)));
    return metadata;
  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    return 0; // TODO implement
  }

  //======================================== Default / No-op implementations ========================================//

  @Override
  public boolean createStream(StreamSpec streamSpec) {
    return true;
  }

  @Override
  public void validateStream(StreamSpec streamSpec) throws StreamValidationException {

  }

  @Override
  public boolean clearStream(StreamSpec streamSpec) {
    return true;
  }

  @Override
  public void deleteMessages(Map<SystemStreamPartition, String> offsets) {

  }

  @Override
  public Set<SystemStream> getAllSystemStreams() {
    return Collections.emptySet();
  }
}
