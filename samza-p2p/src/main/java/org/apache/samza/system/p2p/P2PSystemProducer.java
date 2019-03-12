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
import com.google.common.primitives.Longs;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.p2p.checkpoint.CheckpointWatcher;
import org.apache.samza.system.p2p.checkpoint.CheckpointWatcherFactory;
import org.apache.samza.system.p2p.pq.PersistentQueue;
import org.apache.samza.system.p2p.pq.PersistentQueueFactory;
import org.apache.samza.system.p2p.pq.PersistentQueueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class P2PSystemProducer implements SystemProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(P2PSystemProducer.class);
  private final String systemName;
  private final int producerId;
  private final PersistentQueueFactory persistentQueueFactory;
  private final CheckpointWatcher checkpointWatcher;
  private final Config config;
  private final MetricsRegistry metricsRegistry;
  private final JobInfo jobInfo;

  private final Map<String, PersistentQueue> persistentQueues;
  private final Set<ProducerConnectionHandler> connectionHandlers;
  private final AtomicLong nextOffset = new AtomicLong(0L);
  private final AtomicLong minCheckpointedOffset = new AtomicLong(0L);

  P2PSystemProducer(String systemName, int producerId, PersistentQueueFactory persistentQueueFactory,
      CheckpointWatcherFactory checkpointWatcherFactory, Config config, MetricsRegistry metricsRegistry, JobInfo jobInfo) {
    this.systemName = systemName;
    this.producerId = producerId;
    this.persistentQueueFactory = persistentQueueFactory;
    this.checkpointWatcher = checkpointWatcherFactory.getCheckpointWatcher(config, jobInfo.getTasks(), metricsRegistry);
    this.config = config;
    this.metricsRegistry = metricsRegistry;
    this.jobInfo = jobInfo;

    this.persistentQueues = new HashMap<>();
    this.connectionHandlers = new HashSet<>();
  }

  @Override
  public void register(String source) { }

  @Override
  public void start() {
    try {
      for (int consumerId = 0; consumerId < Constants.Common.NUM_CONTAINERS; consumerId++) {
        PersistentQueue persistentQueue =
            persistentQueueFactory.getPersistentQueue(String.valueOf(consumerId), config, metricsRegistry);
        persistentQueues.put(String.valueOf(consumerId), persistentQueue);
        connectionHandlers.add(new ProducerConnectionHandler(producerId, consumerId, persistentQueue));
      }
      connectionHandlers.forEach(Thread::start);
      checkpointWatcher.updatePeriodically(systemName, producerId, minCheckpointedOffset);
    } catch (Exception e) {
      throw new SamzaException("Unable to start P2PSystemConsumer", e);
    }
  }

  @Override
  public void stop() {
    checkpointWatcher.close();
    connectionHandlers.forEach(ProducerConnectionHandler::close);
    persistentQueues.forEach((id, pq) -> pq.close());
  }

  @Override
  public void send(String source, OutgoingMessageEnvelope envelope) {
    byte[] system = envelope.getSystemStream().getSystem().getBytes();
    byte[] stream = envelope.getSystemStream().getStream().getBytes();
    byte[] key = (byte[]) envelope.getKey();
    byte[] message = (byte[]) envelope.getMessage();

    int numPartitions = jobInfo.getNumPartitions();
    int partition = jobInfo.getPartitionFor(key, numPartitions);

    if (key == null || message == null) {
      throw new SamzaException("Key and message must not be null");
    }

    int payloadLength = (4 + system.length) + (4 + stream.length) + 4 + (4 + key.length) + (4 + message.length);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[payloadLength]);
    buffer // TODO verify need message header?
        .put(Ints.toByteArray(system.length)).put(system) // TODO compact ssp representation
        .put(Ints.toByteArray(stream.length)).put(stream)
        .put(Ints.toByteArray(partition))
        .put(Ints.toByteArray(key.length)).put(key)
        .put(Ints.toByteArray(message.length)).put(message);

    long offset = this.nextOffset.get();
    try {
      int destinationConsumerId = jobInfo.getConsumerFor(partition);
      LOGGER.info("Writing message for Consumer: {} with offset: {}", destinationConsumerId, offset);
      persistentQueues.get(String.valueOf(destinationConsumerId))
          .append(Longs.toByteArray(offset), buffer.array());
    } catch (Exception e) {
      throw new SamzaException(String.format("Error putting data for offset: %d in the DB", offset));
    }

    nextOffset.incrementAndGet();
  }

  @Override
  public void flush(String source) {
    LOGGER.info("Flush requested");
    try {
      for (Map.Entry<String, PersistentQueue> mapEntry : persistentQueues.entrySet()) {
        mapEntry.getValue().flush();
      }
    } catch (IOException e) {
      throw new SamzaException("Error flushing persistent queues.", e);
    }

    long currentNextOffset = nextOffset.get();
    // TODO verify check for per task lastSentOffset?
    long offset = minCheckpointedOffset.get();
    while (offset < currentNextOffset) {
      try {
        Thread.sleep(1000); // TODO add upper bounds
        offset = minCheckpointedOffset.get();
      } catch (InterruptedException e) {
        throw new SamzaException(e);
      }
    }

    for (Map.Entry<String, PersistentQueue> entry : persistentQueues.entrySet()) {
      PersistentQueue pq = entry.getValue();
      try {
        pq.deleteUpto(Longs.toByteArray(offset));
      } catch (IOException e) {
        throw new SamzaException(
            String.format("Error cleaning up persistent queue for data up to committed offset: %s.", offset), e);
      }
    }
  }

  private static class ProducerConnectionHandler extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConnectionHandler.class);
    private static final int CONNECTION_RETRY_INTERVAL = 1000;
    private static final int CONNECTION_TIMEOUT = 1000;
    private static final int SEND_WAIT_MS = 10;

    private final int producerId;
    private final int consumerId;
    private final PersistentQueue persistentQueue;
    private Socket socket = new Socket();
    private volatile boolean shutdown = false;

    ProducerConnectionHandler(int producerId, int consumerId, PersistentQueue persistentQueue) {
      super("ProducerConnectionHandler " + consumerId);
      this.producerId = producerId;
      this.consumerId = consumerId;
      this.persistentQueue = persistentQueue;
    }

    public void run() {
      LOGGER.info("ProducerConnectionHandler handler to Consumer: {} for Producer: {} is now running.", consumerId, producerId);
      try {
        while (!shutdown && !socket.isConnected()) {
          socket.setTcpNoDelay(true);
          try {
            // read the consumer port from file every time.
            long consumerPort = Util.readFileLong(Constants.Common.getConsumerPortPath(consumerId));
            socket.connect(new InetSocketAddress(Constants.Common.SERVER_HOST, (int) consumerPort), CONNECTION_TIMEOUT);
            LOGGER.info("Connected to Consumer: {} at Port: {} in Producer: {}", consumerId, consumerPort, producerId);
            send(socket); // blocks
          } catch (Exception ce) {
            LOGGER.error("Error in connection to Consumer: {} in Producer: {}", consumerId, producerId, ce);
            this.socket = new Socket();
            LOGGER.info("Retrying connection to Consumer: {} in Producer: {}", consumerId, producerId);
            Thread.sleep(CONNECTION_RETRY_INTERVAL);
          }
        }
      } catch (Exception e) {
        throw new SamzaException("Error in ProducerConnectionHandler to Consumer: " + consumerId + " in Producer: " + producerId, e);
      }
    }

    void close() {
      shutdown = true;
      try {
        socket.close();
      } catch (IOException e) {
        throw new SamzaException(
            String.format("Error closing socket to Consumer: %s in Producer: %s", consumerId, producerId), e);
      }
    }

    private void send(Socket socket) throws Exception {
      DataInputStream inputStream = new DataInputStream(socket.getInputStream());
      OutputStream outputStream = socket.getOutputStream();

      sync(outputStream);

      byte[] startingOffset = null;
      while (!Thread.currentThread().isInterrupted()) {
        byte[] lastSentOffset = sendSince(startingOffset, outputStream);
//        LOGGER.info("Last sent offset: {} for Producer: {}", Longs.fromByteArray(lastSentOffset), producerId);
        if (lastSentOffset != startingOffset) {
          startingOffset = Longs.toByteArray(Longs.fromByteArray(lastSentOffset) + 1);
        }
//        LOGGER.info("Next starting offset: {} for Producer: {}", Longs.fromByteArray(startingOffset), producerId);
        Thread.sleep(SEND_WAIT_MS);
      }
    }

    /**
     * Synchronize with the consumer on connection establishment:
     *    1. Send producerId to consumer to identify self.
     */
    private void sync(OutputStream outputStream) throws IOException {
      outputStream.write(Constants.Common.OPCODE_SYNC);
      outputStream.write(Ints.toByteArray(producerId));
      outputStream.flush();
    }

    /**
     * Sends all currently available data in the store since the {@code startingOffset} to the Consumer.
     * If {@code startingOffset} is null, sends from the beginning.
     * @return the last offset sent to consumer.
     */
    private byte[] sendSince(byte[] startingOffset, OutputStream outputStream) throws IOException {
      persistentQueue.flush(); // TODO REMOVE
      byte[] lastSentOffset = startingOffset;
      PersistentQueueIterator iterator = persistentQueue.readFrom(startingOffset);

      while (iterator.hasNext()) {
        Pair<byte[], byte[]> entry = iterator.next();
        byte[] storedOffset = entry.getKey();
        byte[] payload = entry.getValue();

        LOGGER.info("Sending data for offset: {} to Consumer: {} from Producer: {}",
            Longs.fromByteArray(storedOffset), consumerId, producerId);
        outputStream.write(Constants.Common.OPCODE_WRITE);
        outputStream.write(storedOffset);
        outputStream.write(payload);
        outputStream.flush(); // TODO remove
        lastSentOffset = storedOffset;
      }

      outputStream.flush();
      iterator.close();
      return lastSentOffset;
    }
  }
}
