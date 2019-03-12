package org.apache.samza.system.p2p;

import com.google.common.primitives.Longs;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
  private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);
  private final Thread produceThread;
  private AtomicLong counter = new AtomicLong(0L);
  private volatile boolean shutdown = false;

  private int processCallCount = 0;

  Task(P2PSystemProducer producer) {
    this.produceThread = new Thread(() -> {
      while(!shutdown && !Thread.currentThread().isInterrupted()) {
        LOGGER.info("Sending message for counter: {}", counter.get());
        byte[] key = Longs.toByteArray(this.counter.get());
        producer.send(Constants.Common.TASK_NAME, new OutgoingMessageEnvelope(Constants.Common.SYSTEM_STREAM, key, key));
        if (this.counter.incrementAndGet() % 10 == 0) {
//          producer.flush(Constants.Common.TASK_NAME);
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) { }
      }
    }, "TaskProducerThread");
  }

  void start() {
    this.produceThread.start();
  }

  void process(List<IncomingMessageEnvelope> imes) {
    imes.forEach(ime -> LOGGER.info("Processing message with offset: {}", ime.getOffset()));

    if (processCallCount++ % 10 == 0) {
      try {
        String offset = imes.get(imes.size() - 1).getOffset();
        LOGGER.info("Writing checkpoint file with offset: {}", offset);
        Util.writeFile(Constants.Common.getTaskCheckpointPath(Constants.Common.TASK_NAME), offset);
      } catch (Exception e) {
        throw new SamzaException("Could not write checkpoint");
      }
    }
    // TODO record and compare
  }

  void stop() {
    this.shutdown = true;
    this.produceThread.interrupt();
  }
}
