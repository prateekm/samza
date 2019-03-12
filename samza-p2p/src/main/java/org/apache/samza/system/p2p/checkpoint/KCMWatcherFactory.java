package org.apache.samza.system.p2p.checkpoint;

import java.util.List;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;

public class KCMWatcherFactory implements CheckpointWatcherFactory {
  public CheckpointWatcher getCheckpointWatcher(Config config, List<TaskName> tasks, MetricsRegistry metricsRegistry) {
    try {
      CheckpointManager checkpointManager =
          ((CheckpointManagerFactory) Class.forName(config.get("task.checkpoint.manager")).newInstance())
              .getCheckpointManager(config, metricsRegistry);

      return new KCMWatcher(checkpointManager, tasks);
    } catch (Exception e) {
      throw new SamzaException("Could not create a KCMWatcher");
    }
  }
}
