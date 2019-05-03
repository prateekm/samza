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
package org.apache.samza.system.p2p.pq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.p2p.Constants;
import org.apache.samza.system.p2p.ProducerOffset;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBPersistentQueue implements PersistentQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBPersistentQueue.class);
  private final String name;
  private final RocksDB db;

  RocksDBPersistentQueue(String name, Config config, MetricsRegistry metricsRegistry) throws Exception {
    this.name = name;
    String storePath = Constants.getPersistentQueueBasePath(name);
    Files.createDirectories(Paths.get(storePath).getParent());
    this.db = RocksDB.open(Constants.DB_OPTIONS, storePath);
  }

  @Override
  public void append(ProducerOffset id, byte[] message) throws IOException {
    try {
      db.put(id.getBytes(), message);
    } catch (RocksDBException e) {
      throw new IOException(String.format("Error appending data to db for queue: %s", name), e);
    }
  }

  @Override
  public void flush() throws IOException {
    try {
      db.flush(Constants.FLUSH_OPTIONS);
    } catch (RocksDBException e) {
      throw new IOException(String.format("Error flushing data to db for queue: %s", name), e);
    }
  }

  @Override
  public PersistentQueueIterator readFrom(ProducerOffset startingId) {
    RocksIterator rocksIterator = db.newIterator();
    if (startingId != null) {
      rocksIterator.seek(startingId.getBytes());
    } else {
      rocksIterator.seekToFirst();
    }
    return new RocksDBPersistentQueueIterator(rocksIterator);
  }

  @Override
  public void deleteUpto(ProducerOffset endId) throws IOException {
    // TODO issues with concurrent access / non-existent endId?
    try {
      RocksIterator iterator = db.newIterator();
      iterator.seekToFirst();
      if (iterator.isValid()) {
        ProducerOffset startingId = new ProducerOffset(iterator.key());
        iterator.close();
        if (startingId.compareTo(endId) < 0) {
          LOGGER.trace("Deleting data from startingId: {} to endId: {}", startingId, endId);
          db.deleteRange(startingId.getBytes(), endId.getBytes());
        } else {
          LOGGER.trace("Unexpectedly found startingId: {} to be greater than endId: {}", startingId, endId);
        }
      } else {
        iterator.close();
      }
    } catch (RocksDBException e) {
      throw new IOException(String.format("Error deleting data from queue: %s", name), e);
    }
  }

  @Override
  public void close() {
    db.close();
  }
}
