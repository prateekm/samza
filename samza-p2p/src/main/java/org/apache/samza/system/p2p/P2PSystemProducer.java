/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.system.p2p;

import com.google.common.primitives.Ints;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.checkpoint.CheckpointWatcher;
import org.apache.samza.system.p2p.checkpoint.CheckpointWatcherFactory;
import org.apache.samza.system.p2p.jobinfo.ConsumerLocalityManager;
import org.apache.samza.system.p2p.jobinfo.JobInfo;
import org.apache.samza.system.p2p.pq.PersistentQueue;
import org.apache.samza.system.p2p.pq.PersistentQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class P2PSystemProducer implements SystemProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(P2PSystemProducer.class);

  private final String systemName;
  private final int producerId;
  private final JobInfo jobInfo;
  private final CheckpointWatcher checkpointWatcher;
  private final ConsumerLocalityManager consumerLocalityManager;
  private final ProducerConnectionManager producerConnectionManager;

  private final ConcurrentMap<String, PersistentQueue> persistentQueues;
  private final Set<String> senderTasks;
  private final ConcurrentMap<String, ReadWriteLock> consumerOffsetAndPQReadWriteLocks;
  private volatile boolean shutdown = false;

  /**
   * Offsets are minted as atomic counters on each producer. Each message send (from any task to any p2p ssp)
   * increments the offset and sends it to the appropriate consumer. Hence the offsets from each producer in a SSP
   * aren't contiguous. We assume that processing (checkpointing) within a ssp is in order. I.e., the fact that the
   * (task for the) ssp checkpointed an offset i for the producer means that all messages sent from the producer
   * to the ssp "before" that offset have been processed as well. Note that this applies only within a SSP. I.e.,
   * the fact that offset i appears in an task's ssp's checkpoint does not mean that _all_ messages sent from the
   * producer before that offset have been processed. Specifically, the producer may have sent older messages to a
   * different partition on the same task, a different partition on a different task, or a different stream altogether.
   *
   * However, two conditions still hold:
   * 1. If the minimum producer offset across all downstream partitions _for a stream_ is i, then all messages sent
   *    from the producer _to the stream_ with offset < i have been processed.
   * 2. If the minimum producer offset across all downstream partitions _for all streams_ is i, then _all_ messages sent
   *    from the producer with offset < i have been processed.
   *
   * When the producing task needs to checkpoint/flush, we need to ensure that every message it sent to every ssp until
   * this point has been completely processed. This means that the min producer offset across all p2p ssps needs to be
   * greater than the current next offset.  This alone however isn't sufficient because of persistence of checkpoints
   * across restarts and how we set the nextStartingOffset at startup. More on that below.
   */
  private AtomicReference<ProducerOffset> nextOffset = null; // non final but should only be constructed in start
  private final ConcurrentHashMap<SystemStreamPartition, ProducerOffset> sspLastSentOffset = new ConcurrentHashMap<>();
  // exists because we need both min (during flush) and max (during start) checkpointed offsets
  private final ConcurrentHashMap<SystemStreamPartition, ProducerOffset> sspLastCheckpointedOffset = new ConcurrentHashMap<>();


  P2PSystemProducer(String systemName, int producerId, PersistentQueueFactory persistentQueueFactory,
      CheckpointWatcherFactory checkpointWatcherFactory, ConsumerLocalityManager consumerLocalityManager,
      Config config, MetricsRegistry metricsRegistry, JobInfo jobInfo) {
    this.systemName = systemName;
    this.producerId = producerId;
    this.jobInfo = jobInfo;
    this.checkpointWatcher = checkpointWatcherFactory.getCheckpointWatcher(config, jobInfo.getAllTasks(), metricsRegistry);
    this.consumerLocalityManager = consumerLocalityManager;
    int numConsumers = config.getInt(JobConfig.JOB_CONTAINER_COUNT());
    this.persistentQueues = createPersistentQueues(numConsumers, producerId, persistentQueueFactory, config, metricsRegistry);
    this.senderTasks = new ConcurrentSkipListSet<>();
    this.consumerOffsetAndPQReadWriteLocks = new ConcurrentHashMap<>();

    for (int i = 0; i < numConsumers; i++) {
      consumerOffsetAndPQReadWriteLocks.put(String.valueOf(i), new ReentrantReadWriteLock());
    }

    this.producerConnectionManager = new ProducerConnectionManager(numConsumers, producerId, consumerLocalityManager,
        persistentQueues, consumerOffsetAndPQReadWriteLocks, config, metricsRegistry);
  }

  @Override
  public void register(String taskName) { }

  @Override
  public void start() {
    LOGGER.info("Starting P2PSystemProducer: {}", producerId);
    consumerLocalityManager.start();
    checkpointWatcher.start(systemName, producerId, jobInfo, sspLastCheckpointedOffset);
    this.nextOffset = getInitialNextOffset();
    producerConnectionManager.start();
    LOGGER.info("Started P2PSystemProducer: {}", producerId);
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping P2PSystemProducer: {}", producerId);
    this.shutdown = true;
    producerConnectionManager.stop();
    checkpointWatcher.stop();
    persistentQueues.forEach((id, pq) -> pq.close());
    consumerLocalityManager.stop();
    LOGGER.info("Stopped P2PSystemProducer: {}", producerId);
  }

  @Override
  public void send(String taskName, OutgoingMessageEnvelope envelope) {
    byte[] system = envelope.getSystemStream().getSystem().getBytes();
    byte[] stream = envelope.getSystemStream().getStream().getBytes();
    byte[] key = (byte[]) envelope.getKey();
    byte[] message = (byte[]) envelope.getMessage();
    int partition = jobInfo.getPartitionFor(key);
    SystemStreamPartition destinationSSP = // TODO maybe can exclude system name
        new SystemStreamPartition(
            envelope.getSystemStream().getSystem(),
            envelope.getSystemStream().getStream(),
            new Partition(partition));

    if (key == null || message == null) {
      throw new SamzaException("Key and message must not be null");
    }

    int payloadLength = (4 + system.length) + (4 + stream.length) + 4 + (4 + key.length) + (4 + message.length);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[payloadLength]);
    buffer // TODO verify need message header / protocol version / length field
        .put(Ints.toByteArray(system.length)).put(system) // TODO compact ssp representation
        .put(Ints.toByteArray(stream.length)).put(stream)
        .put(Ints.toByteArray(partition))
        .put(Ints.toByteArray(key.length)).put(key)
        .put(Ints.toByteArray(message.length)).put(message);

    senderTasks.add(taskName);
    String destinationConsumerId = String.valueOf(jobInfo.getConsumerFor(destinationSSP));

    /**
     * There are (at least) two concurrency issues we need to handle here (if job.container.thread.pool.size > 1
     * or if task.max.concurrency > 1 or multiple concurrent sends from within a process() call)
     *
     * 1. For the sspLastSentOffset update, we need to ensure that the value doesn't regress since we allow commit
     * to progress as soon as the last sent offset <= min checkpointed offset. Consider the following order of events:
     *    Task A gets nextOffset 1 for a message intended for Task i
     *    Task B gets nextOffset 2 for a message intended for Task i
     *    Task B sets sspLastSentOffset for Task i to 2
     *    Task A overwrites sspLastSentOffset for Task i to 1
     *    Task A and B send the message
     *    Task A and B commit, and wait for last sent offset <= min checkpointed offset for Task i
     * The commit make progress as soon as message 1 is checkpointed by Task i instead of waiting for message 2.
     * This can happen if job.container.thread.pool.size > 1
     *
     * 2. For the producer append, we need to ensure that appended offset does not regress. Consider the following
     * order of events:
     *    Task A gets nextOffset 1 for a message intended for Task i
     *    Task B (or Task A with task.max.concurrency > 1) gets nextOffset 2 for a message intended for Task i
     *    Task B writes message to queue
     *    ProducerConnectionHandler for Task i iterates over queue from last sent to current end message and resets last sent = current end.
     *    Task A writes message to the queue.
     * ProducerConnectionHandler is going to ignore/lose Task A's message.
     *
     * For now, we synchronize the whole sequence. If this becomes a bottleneck, there might be ways to optimize this. E.g.,
     * For 1. Only set lastSentOffset if larger than current
     * For 2. Wrap getNextOffset in something that can track low watermark (safe offset to commit). Task send can
     * "lease" an offset, make the store write, return the offset. ProducerConnectionHandler can send only up to
     * low watermark instead of store end.
     * For 2. Use a reader writer lock to allow concurrent appends (reader) from Tasks but exclusive scans (writer)
     * from ProducerConnectionHandler. Less desirable since we're locking while sending messages synchronously over
     * network or buffering several messages in memory. // TODO maybe not that bad now since we only need to lock when creating new iterator?
     */
    Lock consumerReadLock = consumerOffsetAndPQReadWriteLocks.get(destinationConsumerId).readLock();
    ProducerOffset offset = this.nextOffset.get();
    try {
      consumerReadLock.lock(); // has to be before next offset get, not just queue append
      while (!this.nextOffset.compareAndSet(offset, ProducerOffset.nextOffset(offset))) {
        offset = this.nextOffset.get();
      }

      if (offset.getMessageId() % 1000 == 0) {
        LOGGER.debug("Persisting message with offset: {} for Consumer: {}", offset, destinationConsumerId);
      }
      LOGGER.trace("Persisting message with offset: {} for Consumer: {}", offset, destinationConsumerId);

      try {
        persistentQueues.get(destinationConsumerId).append(ProducerOffset.toBytes(offset), buffer.array());
      } catch (Exception e) {
        throw new SamzaException(String.format("Error appending data for offset: %s to the queue.", offset), e);
      }
    } finally {
      consumerReadLock.unlock();
    }

    final ProducerOffset finalOffset = offset;
    sspLastSentOffset.compute(destinationSSP, (ssp, currentOffset) -> {
      if (currentOffset == null) return finalOffset;
      if (ProducerOffset.compareTo(finalOffset, currentOffset) > 0) {
        return finalOffset;
      } else {
        return currentOffset;
      }
    });
  }

  @Override
  public void flush(String taskName) {
    if (!senderTasks.contains(taskName)) {
      return;
    }

    LOGGER.debug("Flush requested from task: {}", taskName);
    for (Map.Entry<String, PersistentQueue> entry : persistentQueues.entrySet()) {
      String queueName = entry.getKey();
      try {
        entry.getValue().flush();
      } catch (IOException e) {
        throw new SamzaException(String.format("Error flushing persistent queue: %s", queueName), e);
      }
    }

    Map<SystemStreamPartition, ProducerOffset> currentLastTaskSentOffsets = new HashMap<>(sspLastSentOffset);
    boolean isUpToDate = false;
    while (!isUpToDate) {
      isUpToDate = true;
      for (Map.Entry<SystemStreamPartition, ProducerOffset> e: sspLastCheckpointedOffset.entrySet()) {
        SystemStreamPartition ssp = e.getKey();
        ProducerOffset lastTaskCheckpointedOffset = e.getValue();
        ProducerOffset lastTaskSentOffset = currentLastTaskSentOffsets.get(ssp);
        if (lastTaskSentOffset != null
            && ProducerOffset.compareTo(lastTaskSentOffset, lastTaskCheckpointedOffset) > 0) {
          LOGGER.info("Blocking flush for task: {} since destination ssp: {} in Consumer: {} lastSentOffset: {} is more than lastCheckpointedOffset: {}",
              taskName, ssp, jobInfo.getConsumerFor(ssp), lastTaskSentOffset, lastTaskCheckpointedOffset);
          isUpToDate = false;
          break;
        }
      }

      try {
        Thread.sleep(Constants.PRODUCER_FLUSH_SLEEP_MS); // TODO add upper bounds, break on shutdown
      } catch (InterruptedException e) {
        throw new SamzaException("Interrupted while waiting for checkpoint to progress.", e);
      }
    }

    ProducerOffset currentMinCheckpointedOffset = getMinCheckpointedOffset();
    LOGGER.debug("Deleting data up to offset: {}", currentMinCheckpointedOffset);
    for (Map.Entry<String, PersistentQueue> entry : persistentQueues.entrySet()) {
      String queueName = entry.getKey();
      try {
        entry.getValue().deleteUpto(ProducerOffset.toBytes(currentMinCheckpointedOffset));
      } catch (IOException e) {
        throw new SamzaException(
            String.format("Error cleaning up persistent queue: %s for data up to committed offset: %s.",
                queueName, currentMinCheckpointedOffset), e);
      }
    }

    LOGGER.debug("Flush completed for task: {}", taskName);
  }

  /**
   * The checkpointed offsets for a task at producer start may be:
   * 0. null (only if error reading checkpoint)
   * 1. empty (only for a new job)
   * 2. present but stale (for a job with offset reset enabled
   * 3. present and valid (for a regular job after regular shutdown)
   *
   * for 0, we should retry until we're able to read checkpoint.
   * for 1, we can set nextOffset to the min value.
   * for 2 & 3 we can set it to max of all checkpointed offsets
   *
   * Invariant: flushing immediately after start without sending any messages should always succeed
   * since initial task last sent offset == null (it's created on first send), this works fine.
   *
   * Why track lastSentOffset per ssp? Let's say we only tracked the producer-wide last sent offset and compared
   * that against the min checkpoint on commit. Consider the case when only one Task sent one message then flushed,
   * then task last sent offset = nextOffset , but we'd still be waiting for min to progress in checkpoint which
   * won't happen until messages are sent to each ssp. Similarly, let's say we tracked the last sent offset per
   * destination task. Consider the case where a producer sends two messages with offsets 1 and 2 to different
   * ssps on the same destination task. Since there is no processing order guarantee across partitions, the
   * destination task may process message 2 and checkpoint and then process message 1, leading to a false start.
   * Hence we need to track last sent offset per destination ssp instead of per producer or per task.
   *
   * Why include time based epoch: Let's say we set the initial nextOffset to maxCheckpointedOffset.
   * If consumer has messages buffered from previous producer but not checkpointed yet, we set nextOffset
   * based on the max checkpointed offset and send message then flush and wait, consumer processes from buffer
   * and checkpoints stale offset > min, we will unblock commit when we shouldn't. To prevent this, the producer
   * offset needs to be monotonically increasing, even across restarts. Instead of relying on an external
   * strongly consistent durable store, we use the current time millis at producer start as an 'approximate' but
   * good enough monotonic counter. We make the assumption that the current time millis will not regress across
   * producer restarts, even on different machines (i.e., clock skew < producer restart time, no machines with
   * bad clocks). We can try to detect violations of this assumption and fix (by waiting more) or fail hard.
   */
  private AtomicReference<ProducerOffset> getInitialNextOffset() {
    while (sspLastCheckpointedOffset.get(Constants.CHECKPOINTS_READ_ONCE_DUMMY_KEY) == null) {
      try {
        LOGGER.trace("Waiting for last task checkpoint offset to be updated.");
        Thread.sleep(1000);
      } catch (InterruptedException e) { }
    }

    ProducerOffset maxCheckpointedOffset = getMaxCheckpointedOffset();
    long prevEpoch = maxCheckpointedOffset.getEpoch();
    // additional validation to ensure that current epoch is greater than last checkpointed epoch.
    // it's still possible (although unlikely) that the epoch for previously delivered in-memory messages
    // on the consumer is higher (e.g. due to a bad clock).
    // TODO maybe we can detect this on the consumer and reject writes?
    long currentEpoch = System.currentTimeMillis();
    while (currentEpoch < prevEpoch + 1) {
      LOGGER.error("Current producer epoch: {} is less than the previous producer epoch: {}", currentEpoch, prevEpoch);
      try {
        Thread.sleep(prevEpoch - currentEpoch);
        currentEpoch = System.currentTimeMillis();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    ProducerOffset offset = new ProducerOffset(currentEpoch, 0);
    LOGGER.info("New initial offset: {} in Producer: {}", offset, producerId);
    return new AtomicReference<>(offset);
  }

  private ConcurrentMap<String, PersistentQueue> createPersistentQueues(int numConsumers, int producerId,
      PersistentQueueFactory factory, Config config, MetricsRegistry metricsRegistry) {
    ConcurrentMap<String, PersistentQueue> persistentQueues = new ConcurrentHashMap<>();
    try {
      for (int consumerId = 0; consumerId < numConsumers; consumerId++) {
        String persistentQueueName = producerId + "-" + consumerId;

        try {
          FileUtils.deleteDirectory(new File(Constants.getPersistentQueueBasePath(persistentQueueName))); // clear old state first
        } catch (Exception e) {
          LOGGER.error("Error clearing persistent queue for Consumer: {} in Producer: {}", consumerId, producerId, e);
        }

        PersistentQueue persistentQueue =
            factory.getPersistentQueue(persistentQueueName, config, metricsRegistry);
        persistentQueues.put(String.valueOf(consumerId), persistentQueue);
      }
      return persistentQueues;
    } catch (Exception e) {
      throw new SamzaException("Unable to create persistent queues", e);
    }
  }

  private ProducerOffset getMaxCheckpointedOffset() {
    // guaranteed to contain at least the dummy value (hasBeenUpdatedOnce)
    return sspLastCheckpointedOffset.values().stream().max(ProducerOffset::compareTo).get();
  }

  private ProducerOffset getMinCheckpointedOffset() {
    return sspLastCheckpointedOffset.values().stream() // first filter the dummy value (hasBeenUpdatedOnce)
        .filter(v -> ProducerOffset.compareTo(v, ProducerOffset.MIN_VALUE) > 0)
        .min(ProducerOffset::compareTo)
        .orElse(ProducerOffset.MIN_VALUE);
  }
}