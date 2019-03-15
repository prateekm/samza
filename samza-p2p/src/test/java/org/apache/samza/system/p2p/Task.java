package org.apache.samza.system.p2p;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Random;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
  private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);
  private static final SystemStream SYSTEM_STREAM = Constants.SYSTEM_STREAM;
  private static final Random RANDOM = new Random();

  private final String taskName;
  private final JobInfo jobInfo;
  private final Thread produceThread;
  private final Thread commitThread;

  private volatile String lastReceivedOffset = null;
  private volatile boolean shutdown = false;

  Task(String taskName, P2PSystemProducer producer, JobInfo jobInfo) {
    this.taskName = taskName;
    this.jobInfo = jobInfo;
    this.produceThread = new Thread(() -> {
      while(!shutdown && !Thread.currentThread().isInterrupted()) {
        int keyLength = RANDOM.nextInt(Constants.TASK_MAX_KEY_VALUE_LENGTH);
        int valueLength = RANDOM.nextInt(Constants.TASK_MAX_KEY_VALUE_LENGTH);
        byte[] key = new byte[keyLength];
        byte[] value = new byte[valueLength];
        RANDOM.nextBytes(key);
        RANDOM.nextBytes(value);
        producer.send(taskName, new OutgoingMessageEnvelope(SYSTEM_STREAM, key, value));

        try {
          Thread.sleep(Constants.TASK_PRODUCE_INTERVAL);
        } catch (InterruptedException e) { }
      }
    }, "TaskProduceThread " + taskName);

    this.commitThread = new Thread(() -> {
      while(!shutdown && !Thread.currentThread().isInterrupted()) {
        LOGGER.info("Flushing producer for task: {}.", taskName);
        long startTime = System.currentTimeMillis();
        producer.flush(taskName);
        LOGGER.info("Took {} ms to flush for task {}", System.currentTimeMillis() - startTime, taskName);


        String currentLastReceivedOffset = this.lastReceivedOffset;
        if (currentLastReceivedOffset != null) {
          LOGGER.info("Writing checkpoint file with offset: {}", currentLastReceivedOffset);
          try {
            Util.writeFile(Constants.getTaskCheckpointPath(taskName), currentLastReceivedOffset);
          } catch (Exception e) {
            throw new SamzaException("Could not write checkpoint file.", e);
          }
        }

        try {
          Thread.sleep(Constants.TASK_FLUSH_INTERVAL);
        } catch (InterruptedException e) { }
      }
    }, "TaskCommitThread " + taskName);
  }

  void start() {
    this.produceThread.start();
    this.commitThread.start();
  }

  void process(List<IncomingMessageEnvelope> imes) {
    imes.forEach(ime -> {
      LOGGER.trace("Processing polled message with offset: {} in task: {}", ime.getOffset(), taskName);
      int partition = jobInfo.getPartitionFor((byte[]) ime.getKey());
      Preconditions.checkState(("Partition " + partition).equals(taskName));
      // TODO record data / add more asserts
    });

    this.lastReceivedOffset = imes.get(imes.size() - 1).getOffset();
  }

  void stop() {
    this.shutdown = true;
    this.produceThread.interrupt();
    this.commitThread.interrupt();
    this.commitThread.interrupt();
  }
}
