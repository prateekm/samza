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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.jobinfo.ConsumerLocalityManager;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class P2PSystemConsumer extends BlockingEnvelopeMap {
  private static final Logger LOGGER = LoggerFactory.getLogger(P2PSystemConsumer.class);
  private final int consumerId;
  private final Set<ConsumerConnectionHandler> connectionHandlers;
  private final MessageSink messageSink;
  private final AtomicLongArray producerOffsets;
  private final Thread acceptorThread;

  public P2PSystemConsumer(int consumerId, Config config, MetricsRegistry metricsRegistry, Clock clock,
      ConsumerLocalityManager consumerLocalityManager) {
    super(metricsRegistry, clock);
    this.consumerId = consumerId;
    this.connectionHandlers = new LinkedHashSet<>();
    this.messageSink = new MessageSink(this);
    this.producerOffsets = new AtomicLongArray(new long[config.getInt(JobConfig.JOB_CONTAINER_COUNT())]);
    this.acceptorThread = new Thread(() -> {
        ConsumerConnectionHandler connectionHandler = null;
        try (ServerSocket serverSocket = new ServerSocket()) {
          serverSocket.bind(null);
          int consumerPort = serverSocket.getLocalPort();

          LOGGER.info("Writing Port: {} for Consumer: {}", consumerPort, consumerId);
          consumerLocalityManager.start();
          consumerLocalityManager.writeConsumerPort(String.valueOf(consumerId), consumerPort);
          consumerLocalityManager.stop();
          while (!Thread.currentThread().isInterrupted()) {
            Socket socket = serverSocket.accept();
            connectionHandler = new ConsumerConnectionHandler(consumerId, socket, producerOffsets, messageSink);
            connectionHandlers.add(connectionHandler);
            connectionHandler.start();
          }
          LOGGER.info("Exiting connection accept loop in Consumer: {}", consumerId);
        } catch (Exception e) {
          throw new RuntimeException("Error handling connection in Consumer." + consumerId, e);
        }
      }, "ConsumerConnectionAcceptor " + consumerId);
  }

  @Override
  public void start() {
    LOGGER.info("Consumer: {} is starting.", consumerId);
    acceptorThread.start();
  }

  @Override
  public void stop() {
    acceptorThread.interrupt();
    connectionHandlers.forEach(ConsumerConnectionHandler::close);
    // TODO close socket? isShuttingDown boolean?
  }

  private static class ConsumerConnectionHandler extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerConnectionHandler.class);
    private final int consumerId;
    private final Socket socket;
    private final AtomicLongArray producerOffsets;
    private final MessageSink messageSink;

    private int producerId;
    private volatile boolean shutdown = false;

    ConsumerConnectionHandler(int consumerId, Socket socket, AtomicLongArray producerOffsets, MessageSink messageSink) {
      super("ConsumerConnectionHandler " + consumerId);
      this.consumerId = consumerId;
      this.socket = socket;
      this.producerOffsets = producerOffsets;
      this.messageSink = messageSink;
    }

    @Override
    public void run() {
      try {
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        final byte[] opCode = new byte[4]; // 4 == OPCODE length

        while (!shutdown && !Thread.currentThread().isInterrupted()) {
          inputStream.readFully(opCode);

          switch (Ints.fromByteArray(opCode)) {
            case Constants.OPCODE_SYNC_INT:
              handleSync(inputStream);
              break;
            case Constants.OPCODE_WRITE_INT:
              handleWrite(inputStream);
              break;
            default:
              throw new UnsupportedOperationException("Unknown opCode: " + Ints.fromByteArray(opCode) + " in Consumer: " + consumerId);
          }
        }
      } catch (EOFException | SocketException e) {
        LOGGER.info("Shutting down connection handler in Consumer: {} due to connection close.", consumerId);
      } catch (Exception e) {
        LOGGER.info("Error in connection handler in Consumer: {}", consumerId, e);
        throw new SamzaException(e);
      } finally {
        try {
          socket.close();
        } catch (Exception e) {
          LOGGER.info("Error during ConsumerConnectionHandler shutdown in Consumer: {}", consumerId, e);
        }
      }
    }

    public void close() {
      this.shutdown = true;
      this.interrupt();
    }

    private void handleSync(DataInputStream inputStream) throws IOException {
      byte[] producerId = new byte[4];
      inputStream.readFully(producerId);
      this.producerId = Ints.fromByteArray(producerId);
    }

    private void handleWrite(DataInputStream inputStream) throws IOException, InterruptedException {
      long producerOffset = inputStream.readLong();
      LOGGER.trace("Received write request from producer: {} with offset: {} in Consumer: {}", producerId, producerOffset, consumerId);

      int systemLength = inputStream.readInt();
      byte[] systemBytes = new byte[systemLength];
      inputStream.readFully(systemBytes);

      int streamLength = inputStream.readInt();
      byte[] streamBytes = new byte[streamLength];
      inputStream.readFully(streamBytes);

      int partition = inputStream.readInt();
      String systemName = new String(systemBytes);
      String streamName = new String(streamBytes);
      SystemStreamPartition ssp = new SystemStreamPartition(systemName, streamName, new Partition(partition));

      LOGGER.trace("Received write request for ssp: {} in Consumer: {}", ssp, consumerId);

      int keyLength = inputStream.readInt();
      byte[] keyBytes = new byte[keyLength];
      inputStream.readFully(keyBytes);

      int messageLength = inputStream.readInt();
      byte[] messageBytes = new byte[messageLength];
      inputStream.readFully(messageBytes);

      producerOffsets.set(producerId, producerOffset);
      String sspOffset = producerOffsets.toString(); // TODO verify if approx / non atomic OK.
      IncomingMessageEnvelope ime = new IncomingMessageEnvelope(ssp, sspOffset, keyBytes, messageBytes);

      try {
        messageSink.put(ssp, ime);
      } catch (Exception e) {
        LOGGER.error("Error putting IME: {} for SSP: {} in BEM", ime, ssp);
        throw e;
      }
    }
  }

  private class MessageSink {
    private final P2PSystemConsumer consumer;

    MessageSink(P2PSystemConsumer consumer) {
      this.consumer = consumer;
    }

    void put(SystemStreamPartition ssp, IncomingMessageEnvelope ime) throws InterruptedException {
      consumer.put(ssp, ime);
    }
  }
}
