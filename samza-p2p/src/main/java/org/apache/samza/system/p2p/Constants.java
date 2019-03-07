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
  public static class Common {
    public static final int EXECUTION_ID = 0;

    public static final int NUM_CONTAINERS = 3;
    public static final Random RANDOM = new Random();

    public static final Options DB_OPTIONS = new Options().setCreateIfMissing(true);
    public static final FlushOptions FLUSH_OPTIONS = new FlushOptions().setWaitForFlush(true);

    public static final String SERVER_HOST = "127.0.0.1";

    public static final int OPCODE_WRITE_INT = 1;
    public static final byte[] OPCODE_WRITE = Ints.toByteArray(OPCODE_WRITE_INT);

    private static final String STATE_BASE_PATH = "state";
    private static final String PERSISTENT_QUEUE_BASE_PATH = "stores/producer";
    private static final String CONSUMER_PORTS_BASE_PATH = "ports";

    public static String getPersistentQueueBasePath(String queueName) {
      return STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + PERSISTENT_QUEUE_BASE_PATH + "/" + queueName;
    }

    public static Path getConsumerPortPath(int consumerId) {
      return Paths.get(STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + CONSUMER_PORTS_BASE_PATH + "/consumer/" + consumerId + "/PORT");
    }
  }
}
