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

import com.google.common.primitives.Longs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import org.apache.samza.Partition;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.system.SystemStreamPartition;

public class Util {
  public static long readFile(Path filePath) {
    try {
      return Files.isReadable(filePath)
          ? Longs.fromByteArray(Files.readAllBytes(filePath))
          : 0L;
    } catch (IOException | IllegalArgumentException e) {
      return 0L;
    }
  }

  public static void writeFile(Path filePath, long content) throws Exception {
    Files.createDirectories(filePath.getParent());
    Files.write(
        filePath,
        Longs.toByteArray(content),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE,
        StandardOpenOption.SYNC);
  }

  public static void rmrf(String path) throws IOException {
    Files.walk(Paths.get(path))
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
  }

  public static int toPositive(int number) {
    return number & 0x7fffffff;
  }

  /**
   * COPIED FROM KAFKA.
   * Generates 32 bit murmur2 hash from byte array
   * @param data byte array to hash
   * @return 32 bit hash of the given array
   */
  public static int murmur2(final byte[] data) {
    int length = data.length;
    int seed = 0x9747b28c;
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.
    final int m = 0x5bd1e995;
    final int r = 24;

    // Initialize the hash to a random value
    int h = seed ^ length;
    int length4 = length / 4;

    for (int i = 0; i < length4; i++) {
      final int i4 = i * 4;
      int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // Handle the last few bytes of the input array
    switch (length % 4) {
      case 3:
        h ^= (data[(length & ~3) + 2] & 0xff) << 16;
      case 2:
        h ^= (data[(length & ~3) + 1] & 0xff) << 8;
      case 1:
        h ^= data[length & ~3] & 0xff;
        h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }

  public static int getConsumerFor(byte[] key, JobModel jobModel) {
    int partition = getPartitionFor(key, getNumPartitions(jobModel));
    // TODO implement; look up task for partition then container for task.
    return partition;
  }

  private static int getPartitionFor(byte[] key, int numPartitions) {
    return toPositive(murmur2(key)) % numPartitions;
  }

  private static int getNumPartitions(JobModel jobModel) {
    // TODO implement; look up num tasks. assume num partitions == num tasks.
    return Constants.Common.NUM_CONTAINERS;
  }
}
