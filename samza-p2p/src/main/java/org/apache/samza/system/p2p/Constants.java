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
import java.util.Random;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;

public class Constants {
  public static class Orchestrator {

    public static int NUM_ITERATIONS = 1;
    public static int TOTAL_RUNTIME_SECONDS = 600;
    public static int MAX_RUNTIME_SECONDS = 30;
    public static int MIN_RUNTIME_SECONDS = 20;
    public static int INTERVAL_BETWEEN_RESTART_SECONDS = 5; // required to allow rocksdb locks to be released
  }

  public static class Task {
    public static final int COMMIT_INTERVAL = 10000;
    public static final int TASK_SLEEP_MS = 0;
    public static final int MAX_NUM_MESSAGES = 100000;
  }

  public static class Common {
    public static int EXECUTION_ID = 0;

    public static final int NUM_CONTAINERS = 3;
    public static final Random RANDOM = new Random();

    public static final Options DB_OPTIONS = new Options().setCreateIfMissing(true);
    public static final FlushOptions FLUSH_OPTIONS = new FlushOptions().setWaitForFlush(true);

    public static final String SERVER_HOST = "127.0.0.1";

    public static final int OPCODE_WRITE_INT = 1;
    public static final byte[] OPCODE_WRITE = Ints.toByteArray(OPCODE_WRITE_INT);

    private static final String STATE_BASE_PATH = "state";
    private static final String TASK_STORE_BASE_PATH = "stores/task";
    private static final String PRODUCER_STORE_BASE_PATH = "stores/producer";
    private static final String COMMITTED_OFFSETS_BASE_PATH = "offsets";
    private static final String CONSUMER_PORTS_BASE_PATH = "ports";

    public static String getTaskStoreBasePath() {
      return STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + TASK_STORE_BASE_PATH;
    }

    public static String getProducerStoreBasePath() {
      return STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + PRODUCER_STORE_BASE_PATH;
    }

    public static Path getTaskOffsetFilePath(int taskId) {
      return Paths.get(STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + COMMITTED_OFFSETS_BASE_PATH + "/task/" + taskId + "/MESSAGE_ID");
    }

    public static Path getProducerOffsetFilePath(int producerId) {
      return Paths.get(STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + COMMITTED_OFFSETS_BASE_PATH + "/producer/" + producerId + "/OFFSET");
    }

    public static Path getConsumerOffsetFilePath(int consumerId) {
      return Paths.get(STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + COMMITTED_OFFSETS_BASE_PATH + "/consumer/" + consumerId + "/OFFSET");
    }

    public static Path getConsumerPortPath(int consumerId) {
      return Paths.get(STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + CONSUMER_PORTS_BASE_PATH + "/consumer/" + consumerId + "/PORT");
    }
  }
}
