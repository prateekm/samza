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

import com.google.common.primitives.Ints;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;

public class Constants {
  public static final String P2P_SYSTEM_NAME = "p2p"; // hardcode to p2p
  public static final String P2P_INPUT_NUM_PARTITIONS_CONFIG_KEY = "p2p.input.num.partitions";

  public static final int PRODUCER_CH_CONNECTION_RETRY_INTERVAL = 1000;
  public static final int PRODUCER_CH_CONNECTION_TIMEOUT = 1000;
  public static final int PRODUCER_CH_SEND_INTERVAL = 100;
  public static final int PRODUCER_CHECKPOINT_WATCHER_INTERVAL = 1000;
  public static final int PRODUCER_FLUSH_SLEEP_MS = 1000;
  public static final SystemStreamPartition CHECKPOINTS_READ_ONCE_DUMMY_KEY =
      new SystemStreamPartition("", "", new Partition(-1));

  public static final Options DB_OPTIONS = new Options().setCreateIfMissing(true);
  public static final FlushOptions FLUSH_OPTIONS = new FlushOptions().setWaitForFlush(true);

  public static final int OPCODE_SYNC = 1;
  public static final int OPCODE_WRITE = 2;
  public static final int OPCODE_HEARTBEAT = 3;

  private static final String STATE_BASE_PATH = "state";
  private static final String PERSISTENT_QUEUE_BASE_PATH = "stores";

  public static String getPersistentQueueBasePath(String queueName) {
    return STATE_BASE_PATH + "/" + PERSISTENT_QUEUE_BASE_PATH + "/" + queueName;
  }

  public static class Test {
    public static final int EXECUTION_ID = 0;
    public static final int TOTAL_RUNTIME_SECONDS = 3600;
    public static final int MIN_RUNTIME_SECONDS = 60;
    public static final int MAX_RUNTIME_SECONDS = 90;
    public static final int INTERVAL_BETWEEN_RESTART_SECONDS = 5;

    public static final int NUM_CONTAINERS = 2; // job.container.count
    public static final int NUM_PARTITIONS = 4; // p2p.input.num.partitions

    public static final int TASK_PRODUCE_INTERVAL = 0;
    public static final int TASK_FLUSH_INTERVAL = 1000;
    public static final int TASK_MAX_KEY_VALUE_LENGTH = 128;

    public static final String SHARED_STATE_BASE_PATH = "/Users/prateekm/code/work/prateekm-samza/state";

    private static final String CHECKPOINTS_BASE_PATH = "checkpoints";
    private static final String CONSUMER_PORTS_BASE_PATH = "ports";

    public static Path getTaskCheckpointPath(String taskName) {
      return Paths.get(SHARED_STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + CHECKPOINTS_BASE_PATH + "/task/" + taskName);
    }

    public static Path getConsumerPortPath(String consumerId) {
      return Paths.get(SHARED_STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + CONSUMER_PORTS_BASE_PATH + "/consumer/" + consumerId + "/PORT");
    }
  }
}