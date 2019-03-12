package org.apache.samza.system.p2p.checkpoint;

import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;

public class FileCheckpointWatcherFactory implements CheckpointWatcherFactory {
  @Override
  public CheckpointWatcher getCheckpointWatcher(Config config, List<TaskName> tasks, MetricsRegistry metricsRegistry) {
    return new FileCheckpointWatcher();
  }
}
