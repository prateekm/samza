package org.apache.samza.system.p2p.pq;

import java.util.Iterator;
import org.apache.commons.lang3.tuple.Pair;

public interface PersistentQueueIterator extends Iterator<Pair<byte[], byte[]>> {
  void close();
}
