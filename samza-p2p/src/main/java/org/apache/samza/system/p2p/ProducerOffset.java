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

public class ProducerOffset implements Comparable<ProducerOffset> {
  public static final int NUM_BYTES = 16;
  public static final ProducerOffset MIN_VALUE = new ProducerOffset(0, 0);
  private final long[] longs;
  private final byte[] bytes;

  public ProducerOffset(long epoch, long mid) {
    this.longs = new long[] {epoch, mid};
    this.bytes = toBytes(new long[] {epoch, mid});
  }

  public ProducerOffset(byte[] bytes) {
    this.bytes = bytes;
    this.longs = toLongs(bytes);
  }

  public ProducerOffset(String offset) {
    String[] parts = offset.split("-");
    long[] longs = new long[parts.length];
    for (int i = 0; i < parts.length; i++) {
      longs[i] = Long.valueOf(parts[i].trim());
    }
    this.longs = longs;
    this.bytes = toBytes(longs);
  }

  public long getEpoch() {
    return longs[0];
  }

  public long getMessageId() {
    return longs[1];
  }

  public byte[] getBytes() {
    // maybe make returned array immutable
    return this.bytes;
  }

  /**
   * Returns a new instance, does not mutate current.
   */
  public ProducerOffset nextOffset() {
    return new ProducerOffset(longs[0], longs[1] + 1);
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

  @Override
  public int compareTo(ProducerOffset other) {
    if (other == null) throw new IllegalArgumentException();
    if (this == other) return 0;
    if (this.longs[0] != other.longs[0]) {
      return Long.compare(this.longs[0], other.longs[0]);
    } else {
      return Long.compare(this.longs[1], other.longs[1]);
    }
  }

  @Override
  public String toString() {
    return Longs.join("-", longs);
  }

  private byte[] toBytes(long[] longs) {
    ByteBuffer buffer = ByteBuffer.allocate(8 * longs.length);
    for (long val : longs) {
      buffer.putLong(val);
    }
    return buffer.array();
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
