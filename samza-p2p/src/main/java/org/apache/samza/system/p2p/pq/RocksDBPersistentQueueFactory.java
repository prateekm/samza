package org.apache.samza.system.p2p.pq;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;

public class RocksDBPersistentQueueFactory implements PersistentQueueFactory {

  @Override
  public PersistentQueue getPersistentQueue(String name, Config config, MetricsRegistry metricsRegistry) throws Exception {
    return new RocksDBPersistentQueue(name, config, metricsRegistry);
  }
}
