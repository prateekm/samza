package org.apache.samza.system.p2p.checkpoint;

import java.util.concurrent.atomic.AtomicLong;

public interface CheckpointWatcher {
  void updatePeriodically(String systemName, int producerId, AtomicLong minCheckpointedOffset); // TODO add interval
  void close();
}
