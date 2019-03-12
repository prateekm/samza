package org.apache.samza.system.p2p;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.p2p.checkpoint.FileCheckpointWatcherFactory;
import org.apache.samza.system.p2p.pq.RocksDBPersistentQueueFactory;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Orchestrator {
  private static final Logger LOGGER = LoggerFactory.getLogger(Orchestrator.class);
  public static final String P2P_SYSTEM_NAME = "p2pSystem";
  public static final int CONTAINER_ID = 0;

  public static void main(String[] args) {
    run();
  }

  private static void run() {
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        LOGGER.error("Uncaught error in thread: " + t, e);
        System.exit(1);
      }
    });
    Map<String, String> configMap = new HashMap<>();
    MapConfig config = new MapConfig(configMap);
    MetricsRegistry metricsRegistry = new NoOpMetricsRegistry();
    JobInfo jobInfo = new SCSTJobInfo();

    P2PSystemProducer producer = new P2PSystemProducer(P2P_SYSTEM_NAME, CONTAINER_ID, new RocksDBPersistentQueueFactory(),
        new FileCheckpointWatcherFactory(), config, metricsRegistry, jobInfo);
    P2PSystemConsumer consumer = new P2PSystemConsumer(CONTAINER_ID, new NoOpMetricsRegistry(), System::currentTimeMillis);

    Container container = new Container(producer, consumer);
    container.start();
  }
}
