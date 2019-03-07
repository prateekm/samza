package org.apache.samza.system.p2p.pq;

import java.io.IOException;

public interface PersistentQueue {
  void append(byte[] id, byte[] message) throws IOException;
  void flush() throws IOException;
  PersistentQueueIterator readFrom(byte[] startingId);
  void deleteUpto(byte[] endId) throws IOException;
  void close();
}
