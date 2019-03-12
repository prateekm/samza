package org.apache.samza.system.p2p;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;

public class Container {
  private final P2PSystemProducer producer;
  private final P2PSystemConsumer consumer;
  private final Set<SystemStreamPartition> pollSet =
      ImmutableSet.of(new SystemStreamPartition("p2pSystem", "p2pStream", new Partition(0)));
  private final Task task;

  Container(P2PSystemProducer producer, P2PSystemConsumer consumer) {
    this.producer = producer;
    this.consumer = consumer;
    this.task = new Task(producer);
  }

  void start() {
    producer.start();
    task.start();

    consumer.register(pollSet.iterator().next(), "");
    Thread pollThread = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Map<SystemStreamPartition, List<IncomingMessageEnvelope>> pollResults = consumer.poll(pollSet, 1000);
          pollResults.forEach((ssp, imes) -> task.process(imes));
        }
      } catch (InterruptedException e) { }
    }, "PollThread");
    pollThread.start();

    consumer.start();
  }

  void stop() {
    consumer.stop();
    producer.stop();
    task.stop();
  }
}
