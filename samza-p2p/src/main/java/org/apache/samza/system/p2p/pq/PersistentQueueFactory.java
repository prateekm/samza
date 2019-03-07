package org.apache.samza.system.p2p.pq;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;

public interface PersistentQueueFactory {
  PersistentQueue getPersistentQueue(String name, Config config, MetricsRegistry metricsRegistry) throws Exception;
}
