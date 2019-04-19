package org.apache.samza.system.p2p.checkpoint;

import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.p2p.Constants;
import org.apache.samza.system.p2p.jobinfo.JobInfo;
import org.apache.samza.system.p2p.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileCheckpointWatcher implements CheckpointWatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileCheckpointWatcher.class);
  private Thread watcher;
  private volatile boolean shutdown = false;

  @Override
  public void updatePeriodically(String systemName, int producerId, JobInfo jobInfo, AtomicLong minCheckpointedOffset) {
    this.watcher = new Thread(() -> {
      while (!shutdown && !Thread.currentThread().isInterrupted()) {
        try {
          long minOffset = Long.MAX_VALUE;
          List<TaskName> tasks = jobInfo.getAllTasks(); // TODO why only for own tasks? shouldn't this be all tasks
          for (TaskName taskName : tasks) {
            if (taskName.getTaskName().startsWith("Source")) continue; // only check checkpoints for sinks

            long[] offsets =
                Util.parseOffsets(Util.readFileString(Constants.getTaskCheckpointPath(taskName.getTaskName())));
            long producerOffset = offsets[producerId];
            if (producerOffset < minOffset) {
              minOffset = producerOffset;
            }
          }

          if (minOffset == Long.MAX_VALUE) {
            throw new SamzaException("Invalid producer offsets in offset files.");
          }
          minCheckpointedOffset.set(minOffset);
          LOGGER.info("Setting producer checkpointed offset to: {}", minOffset);
        } catch (NoSuchFileException e) {
          LOGGER.info("No checkpoint file found. Setting minCheckpointedOffset to 0 assuming first deploy.");
          minCheckpointedOffset.set(0); // TODO extract constant.
        } catch (Exception e) {
          LOGGER.error("Error finding min checkpointed offset for producerId: {}.", producerId, e);
        }
        try {
          Thread.sleep(Constants.PRODUCER_CHECKPOINT_WATCHER_INTERVAL);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    }, "FileCheckpointWatcher " + producerId);
    watcher.start();
  }

  @Override
  public void close() {
    this.shutdown = true;
    this.watcher.interrupt();
  }
}
