package org.apache.samza.system.p2p.checkpoint;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.samza.system.p2p.JobInfo;

public interface CheckpointWatcher {
  void updatePeriodically(String systemName, int producerId, JobInfo jobInfo, AtomicLong minCheckpointedOffset); // TODO add interval?
  void close();
}
