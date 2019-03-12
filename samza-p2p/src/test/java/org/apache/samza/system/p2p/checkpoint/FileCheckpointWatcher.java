package org.apache.samza.system.p2p.checkpoint;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.samza.system.p2p.Constants;
import org.apache.samza.system.p2p.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileCheckpointWatcher implements CheckpointWatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileCheckpointWatcher.class);
  private Thread watcher;

  @Override
  public void updatePeriodically(String systemName, int producerId, AtomicLong minCheckpointedOffset) {
    this.watcher = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          long[] offsets =
              Util.parseOffsets(Util.readFileString(Constants.Common.getTaskCheckpointPath(Constants.Common.TASK_NAME)));
          long producerOffset = offsets[producerId];
          minCheckpointedOffset.set(producerOffset);
          LOGGER.info("Setting producer checkpointed offset to: {}", producerOffset);
        } catch (Exception e) {
          // TODO LOGGER.error("Error reading checkpoint file.", e);
        }
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    }, "FileCheckpointWatcher");
    watcher.start();
  }

  @Override
  public void close() {
    this.watcher.interrupt();
  }
}
