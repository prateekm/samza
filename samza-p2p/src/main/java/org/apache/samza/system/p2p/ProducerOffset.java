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

import java.nio.ByteBuffer;
import java.util.Arrays;

// TODO make methods static, bytes lazy, avoid allocs
public class ProducerOffset {
  public static final int NUM_BYTES = 16;
  public static final ProducerOffset MIN_VALUE = new ProducerOffset(0, 0);
  private final long[] longs;

  public ProducerOffset(long epoch, long mid) {
    this.longs = new long[] {epoch, mid};
  }

  public ProducerOffset(String offset) {
    String[] parts = offset.split("-");
    long[] longs = new long[parts.length];
    for (int i = 0; i < parts.length; i++) {
      longs[i] = Long.valueOf(parts[i].trim());
    }
    this.longs = longs;
  }

  private ProducerOffset(byte[] bytes) {
    this.longs = toLongs(bytes);
  }

  public long getEpoch() {
    return longs[0];
  }

  public long getMessageId() {
    return longs[1];
  }

  /**
   * Returns a new instance, does not mutate current.
   */
  public static ProducerOffset nextOffset(ProducerOffset offset) {
    return new ProducerOffset(offset.longs[0], offset.longs[1] + 1);
  }

  public static byte[] nextOffset(byte[] offset) {
    // todo implement native increment
    return ProducerOffset.toBytes(ProducerOffset.nextOffset(new ProducerOffset(offset)));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ProducerOffset that = (ProducerOffset) o;
    return Arrays.equals(longs, that.longs);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(longs);
  }

  public static int compareTo(ProducerOffset first, ProducerOffset second) {
    if (second == null) throw new IllegalArgumentException();
    if (first == second) return 0;
    if (first.longs[0] != second.longs[0]) {
      return Long.compare(first.longs[0], second.longs[0]);
    } else {
      return Long.compare(first.longs[1], second.longs[1]);
    }
  }

  public static int compareTo(byte[] first, byte[] second) {
    // todo implement native compareTo
    return ProducerOffset.compareTo(new ProducerOffset(first), new ProducerOffset(second));
  }

  @Override
  public String toString() {
    return Longs.join("-", longs);
  }

  public static String toString(byte[] offset) {
    // todo implement native toString
    return new ProducerOffset(offset).toString();
  }

  public static byte[] toBytes(ProducerOffset offset) {
    ByteBuffer buffer = ByteBuffer.allocate(8 * offset.longs.length);
    for (long val : offset.longs) {
      buffer.putLong(val);
    }
    return buffer.array();
  }

  public static ProducerOffset fromBytes(byte[] offset) {
    return new ProducerOffset(offset);
  }

  private long[] toLongs(byte[] bytes) {
    int numLongs = bytes.length / 8;
    long[] longs = new long[numLongs];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    for (int i = 0; i < numLongs; i++) {
      longs[i] = buffer.getLong();
    }
    return longs;
  }
}
