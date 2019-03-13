package org.apache.samza.system.p2p;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.checkpoint.FileCheckpointWatcherFactory;
import org.apache.samza.system.p2p.pq.RocksDBPersistentQueueFactory;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

public class Container {
  static {
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
    System.setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, "hh:mm:ss:SSS");
    System.setProperty(SimpleLogger.SHOW_THREAD_NAME_KEY, "false");
    System.setProperty(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, "true");
    System.setProperty(SimpleLogger.LEVEL_IN_BRACKETS_KEY, "true");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);
  private final int containerId;
  private final P2PSystemProducer producer;
  private final P2PSystemConsumer consumer;
  private final Set<SystemStreamPartition> pollSet;
  private final Map<Integer, Task> tasks;

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
      LOGGER.error("Uncaught exception in Thread: {}. Prematurely exiting process.", t.getName(), e);
      System.exit(1);
    });
    int containerId = Integer.valueOf(args[0]); // == taskId == producerId == consumerId
    Map<String, String> configMap = new HashMap<>();
    MapConfig config = new MapConfig(configMap);
    MetricsRegistry metricsRegistry = new NoOpMetricsRegistry();
    JobInfo jobInfo = new MCMTJobInfo();

    P2PSystemProducer producer = new P2PSystemProducer(Constants.SYSTEM_NAME, containerId, new RocksDBPersistentQueueFactory(),
        new FileCheckpointWatcherFactory(), config, metricsRegistry, jobInfo);
    P2PSystemConsumer consumer = new P2PSystemConsumer(containerId, new NoOpMetricsRegistry(), System::currentTimeMillis);
    Container container = new Container(containerId, producer, consumer, jobInfo);
    container.start();
  }

  private Container(int containerId, P2PSystemProducer producer, P2PSystemConsumer consumer, JobInfo jobInfo) {
    this.containerId = containerId;
    this.producer = producer;
    this.consumer = consumer;
    this.pollSet = jobInfo.getSSPsFor(containerId);
    this.tasks = new HashMap<>();
    jobInfo.getTasksFor(containerId).forEach(taskName -> {
      String taskNameStr = taskName.getTaskName();
      String partitionId = taskNameStr.split("\\s")[1];
      tasks.put(Integer.valueOf(partitionId), new Task(taskNameStr, producer, jobInfo));
    });
  }

  void start() {
    LOGGER.info("Starting Container {}.", containerId);
    producer.start();
    tasks.forEach((partition, task) -> task.start());
    pollSet.forEach(ssp -> consumer.register(ssp, ""));

    Thread pollThread = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Map<SystemStreamPartition, List<IncomingMessageEnvelope>> pollResults = consumer.poll(pollSet, 1000);
          pollResults.forEach((ssp, imes) -> tasks.get(ssp.getPartition().getPartitionId()).process(imes));
        }
      } catch (InterruptedException e) { }
    }, "ContainerPollThread");
    pollThread.start();

    consumer.start(); // blocks
  }

  void stop() {
    consumer.stop();
    producer.stop();
    tasks.forEach((partition, task) -> task.stop());
  }
}
