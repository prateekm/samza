package org.apache.samza.system.p2p;

import com.google.common.primitives.Longs;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final Set<ConsumerConnection> consumerConnections;

  public P2PSystemProducer(int producerId, RocksDB producerDb) {
    this.producerId = producerId;
    this.producerDb = producerDb;
    this.consumerConnections = new LinkedHashSet<>();
  }

  @Override
  public void register(String source) { }

  @Override
  public void start() {
    for (int consumerId = 0; consumerId < Constants.Common.NUM_CONTAINERS; consumerId++) {
      consumerConnections.add(new ConsumerConnection(producerId, consumerId, producerDb));
    }
    consumerConnections.forEach(Thread::start);
  }

  @Override
  public void stop() { }

  @Override
  public void send(String source, OutgoingMessageEnvelope envelope) {
    byte[] key = (byte[]) envelope.getKey();
    byte[] message = (byte[]) envelope.getMessage();

    ByteBuffer buffer;
    if (message != null) {
      buffer = ByteBuffer.wrap(new byte[key.length + message.length]);
      buffer.put(key).put(message);
    } else {
      buffer = ByteBuffer.wrap(new byte[key.length + Constants.Common.DELETE_PAYLOAD.length]);
      buffer.put(key).put(Constants.Common.DELETE_PAYLOAD);
    }

    try {
      producerDb.put(Longs.toByteArray(nextOffset), buffer.array());
    } catch (Exception e) {
      throw new RuntimeException("TODO"); // TODO
    }

    nextOffset++;
  }

  @Override
  public void flush(String source) {

  }

  private static class ConsumerConnection extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerConnection.class);
    private final Integer producerId;
    private final String consumerId;
    private final RocksDB producerDb;

    ConsumerConnection(Integer producerId, String consumerId, RocksDB producerDb) {
      super("ConsumerConnection " + consumerId);
      this.producerId = producerId;
      this.consumerId = consumerId;
      this.producerDb = producerDb;
    }

    public void run() {
      LOGGER.info("ConsumerConnection handler to Consumer: {} for Producer: {} is now running.", consumerId, producerId);
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
        throw new RuntimeException("Error in ConsumerConnection to Consumer: " + consumerId + " in Producer: " + producerId, e);
      }
    }

    private void send(Socket socket) throws Exception {
      DataInputStream inputStream = new DataInputStream(socket.getInputStream());
      OutputStream outputStream = socket.getOutputStream();

      byte[] lastSentOffset = Longs.toByteArray(0); // TODO
      while(!Thread.currentThread().isInterrupted()) {
        lastSentOffset = sendSinceOffset(outputStream, lastSentOffset);
        Thread.sleep(10);
      }
    }

    // returns the last offset sent to consumer (may not be committed)
    private byte[] sendSinceOffset(OutputStream outputStream, byte[] offset) throws IOException {
      byte[] lastSentOffset = offset;
      // send data from DB since provided offset.
      RocksIterator iterator = producerDb.newIterator();
      iterator.seek(offset);
      while(iterator.isValid()) {
        byte[] storedOffset = iterator.key();
        byte[] message = iterator.value();
        byte[] messageKey = new byte[8];
        byte[] messageValue = new byte[8];
        ByteBuffer wrapper = ByteBuffer.wrap(message);
        wrapper.get(messageKey);
        wrapper.get(messageValue);

        LOGGER.debug("Sending data for offset: {} key: {} to Consumer: {} from Producer: {}",
            Longs.fromByteArray(storedOffset), Longs.fromByteArray(messageKey), consumerId, producerId);
        outputStream.write(Constants.Common.OPCODE_WRITE);
        outputStream.write(messageKey);
        outputStream.write(messageValue);
        lastSentOffset = storedOffset;
        iterator.next();
      }
      outputStream.flush();
      iterator.close();
      return lastSentOffset;
    }
  }
}
