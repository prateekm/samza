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
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class P2PSystemProducer implements SystemProducer {
  private final int producerId;
  private final RocksDB producerDb;
  private long nextOffset; // Note: must match default value for an offset if no offset file found
  private final Set<ConnectionHandler> connectionHandlers;

  public P2PSystemProducer(int producerId, RocksDB producerDb) {
    this.producerId = producerId;
    this.producerDb = producerDb;
    this.connectionHandlers = new LinkedHashSet<>();
  }

  @Override
  public void register(String source) { }

  @Override
  public void start() {
    for (int consumerId = 0; consumerId < Constants.Common.NUM_CONTAINERS; consumerId++) {
      connectionHandlers.add(new ConnectionHandler(producerId, consumerId, producerDb));
    }
    connectionHandlers.forEach(Thread::start);
  }

  @Override
  public void stop() { }

  @Override
  public void send(String source, OutgoingMessageEnvelope envelope) {
    byte[] key = (byte[]) envelope.getKey();
    byte[] message = (byte[]) envelope.getMessage();

    if (key == null || message == null) {
      throw new SamzaException("Key and message must not be null");
    }

    ByteBuffer buffer = ByteBuffer.wrap(new byte[4 + key.length + 4 + message.length]); // 4 = key/message length
    buffer.put(Ints.toByteArray(key.length))
        .put(key)
        .put(Ints.toByteArray(message.length))
        .put(message);

    try {
      producerDb.put(Longs.toByteArray(nextOffset), buffer.array());
    } catch (Exception e) {
      throw new SamzaException(String.format("Error putting data for offset: %d in the DB", nextOffset));
    }

    nextOffset++;
  }

  @Override
  public void flush(String source) {
    // TODO implement
  }

  private static class ConnectionHandler extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHandler.class);
    private final int producerId;
    private final int consumerId;
    private final RocksDB producerDb;

    ConnectionHandler(int producerId, int consumerId, RocksDB producerDb) {
      super("ConnectionHandler " + consumerId);
      this.producerId = producerId;
      this.consumerId = consumerId;
      this.producerDb = producerDb;
    }

    public void run() {
      LOGGER.info("ConnectionHandler handler to Consumer: {} for Producer: {} is now running.", consumerId, producerId);
      try {
        Socket socket = new Socket();
        socket.setTcpNoDelay(true);

        while (!socket.isConnected()) {
          try {
            // read the consumer port from file every time.
            long consumerPort = Util.readFile(Constants.Common.getConsumerPortPath(consumerId));
            socket.connect(new InetSocketAddress(Constants.Common.SERVER_HOST, (int) consumerPort), 0);
            LOGGER.info("Connected to Consumer: {} at Port: {} in Producer: {}", consumerId, consumerPort, producerId);
            send(socket); // blocks
          } catch (Exception ce) {
            LOGGER.debug("Retrying connection to Consumer: {} in Producer: {}", consumerId, producerId);
            socket = new Socket();
            Thread.sleep(1000);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Error in ConnectionHandler to Consumer: " + consumerId + " in Producer: " + producerId, e);
      }
    }

    private void send(Socket socket) throws Exception {
      DataInputStream inputStream = new DataInputStream(socket.getInputStream());
      OutputStream outputStream = socket.getOutputStream();

      byte[] startingOffset = null;
      while (!Thread.currentThread().isInterrupted()) {
        startingOffset = Longs.toByteArray(Longs.fromByteArray(sendSince(startingOffset, outputStream)) + 1);
        Thread.sleep(10);
      }
    }

    /**
     * Sends all currently available data in the store since the {@code startingOffset} to the Consumer.
     * If {@code startingOffset} is null, sends from the beginning.
     * @return the last offset sent to consumer.
     */
    private byte[] sendSince(byte[] startingOffset, OutputStream outputStream) throws IOException {
      byte[] lastSentOffset = startingOffset;

      RocksIterator iterator = producerDb.newIterator();
      if (lastSentOffset == null) {
        iterator.seekToFirst();
      } else {
        iterator.seek(startingOffset);
      }

      while (iterator.isValid()) {
        byte[] storedOffset = iterator.key();
        byte[] keyAndMessage = iterator.value();

        LOGGER.trace("Sending data for offset: {} to Consumer: {} from Producer: {}",
            Longs.fromByteArray(storedOffset), consumerId, producerId);
        outputStream.write(Constants.Common.OPCODE_WRITE);
        outputStream.write(storedOffset);
        outputStream.write(keyAndMessage);
        lastSentOffset = storedOffset;
        iterator.next();
      }

      outputStream.flush();
      iterator.close();
      return lastSentOffset;
    }
  }
}
