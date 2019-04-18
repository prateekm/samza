package org.apache.samza.system.p2p.pq;

import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.RocksIterator;

class RocksDBPersistentQueueIterator implements PersistentQueueIterator {
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
