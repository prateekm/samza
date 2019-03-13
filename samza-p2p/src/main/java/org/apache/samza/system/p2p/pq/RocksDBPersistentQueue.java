package org.apache.samza.system.p2p.pq;

import com.google.common.primitives.Longs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.p2p.Constants;
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
  public void append(byte[] id, byte[] message) throws IOException {
    try {
      db.put(id, message);
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
  public PersistentQueueIterator readFrom(byte[] startingId) {
    RocksIterator rocksIterator = db.newIterator();
    if (startingId != null) {
      rocksIterator.seek(startingId);
    } else {
      rocksIterator.seekToFirst();
    }
    return new RocksDBPersistentQueueIterator(rocksIterator);
  }

  @Override
  public void deleteUpto(byte[] endId) throws IOException {
    // TODO issue with concurrent access / non-existent endId?
    try {
      RocksIterator iterator = db.newIterator();
      iterator.seekToFirst();
      if (iterator.isValid()) {
        byte[] startingId = iterator.key();
        iterator.close();
        long startingIdLong = Longs.fromByteArray(startingId); // TODO remove long assumption
        long endIdLong = Longs.fromByteArray(endId);
        if (endIdLong > startingIdLong) {
          LOGGER.trace("Deleting data from startingId: {} to endId: {}", startingId, endId);
          db.deleteRange(startingId, endId);
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

  private class RocksDBPersistentQueueIterator implements PersistentQueueIterator {
    private final RocksIterator iterator;

    RocksDBPersistentQueueIterator(RocksIterator iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.isValid();
    }

    // By virtue of how RocksdbIterator is implemented, the implementation of
    // our iterator is slightly different from standard java iterator next will
    // always point to the current element, when next is called, we return the
    // current element we are pointing to and advance the iterator to the next
    // location (The new location may or may not be valid - this will surface
    // when the next next() call is made, the isValid will fail)
    @Override
    public Pair<byte[], byte[]> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Pair<byte[], byte[]> entry = Pair.of(iterator.key(), iterator.value());
      iterator.next();
      return entry;
    }

    public void close() {
      iterator.close();
    }
  }
}
