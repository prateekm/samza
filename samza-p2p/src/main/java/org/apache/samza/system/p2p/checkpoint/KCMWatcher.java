package org.apache.samza.system.p2p.checkpoint;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.p2p.JobInfo;
import org.apache.samza.system.p2p.Util;

public class KCMWatcher implements CheckpointWatcher {
  private volatile boolean shutdown;
  private Thread watcherThread;
  private CheckpointManager checkpointManager;
  private List<TaskName> allTasks;

  KCMWatcher(CheckpointManager checkpointManager, List<TaskName> allTasks) {
    this.checkpointManager = checkpointManager;
    this.allTasks = allTasks;
  }

  public void updatePeriodically(String systemName, int producerId, JobInfo jobInfo, AtomicLong minCheckpointedOffset) {
    allTasks.forEach(checkpointManager::register);
    checkpointManager.start();
    this.watcherThread = new Thread(() -> {
      while (!shutdown && !Thread.currentThread().isInterrupted()) {
        long minOffset = allTasks.stream()
            .map(checkpointManager::readLastCheckpoint)
            .map(cp -> cp.getOffsets().entrySet().stream()
                .filter(e -> e.getKey().getSystemStream().getSystem().equals(systemName))
                .map(Map.Entry::getValue).findFirst()
                .get())
            .min(Comparator.comparingLong(o -> Util.parseOffsets(o)[producerId]))
            .map(Long::valueOf).get();
        minCheckpointedOffset.set(minOffset);
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {} // ignore
      }
    });
    watcherThread.start();
  }

  public void close() {
    this.shutdown = true;
    this.watcherThread.interrupt();
    checkpointManager.stop();
  }
}
