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
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class P2PSystemConsumer extends BlockingEnvelopeMap {
  private static final Logger LOGGER = LoggerFactory.getLogger(P2PSystemConsumer.class);
  private final int consumerId;
  private final Set<ConnectionHandler> connectionHandlers;

  public P2PSystemConsumer(int consumerId, MetricsRegistry metricsRegistry, Clock clock) {
    super(metricsRegistry, clock);
    this.consumerId = consumerId;
    this.connectionHandlers = new LinkedHashSet<>();
  }

  @Override
  public void start() {
    LOGGER.info("Consumer: {} is starting.", consumerId);
    ConnectionHandler connectionHandler = null;
    try (ServerSocket serverSocket = new ServerSocket()) {
      serverSocket.bind(null);
      int consumerPort = serverSocket.getLocalPort();
      Util.writeFile(Constants.Common.getConsumerPortPath(consumerId), consumerPort);

      while (!Thread.currentThread().isInterrupted()) {
        Socket socket = serverSocket.accept();
        connectionHandler = new ConnectionHandler(consumerId, socket);
        connectionHandlers.add(connectionHandler);
        connectionHandler.start();
      }
      LOGGER.info("Exiting connection accept loop in Consumer: {}", consumerId);
    } catch (Exception e) {
      throw new RuntimeException("Error handling connection in Consumer." + consumerId, e);
    }
  }

  @Override
  public void stop() {
    // TODO close connection handlers, close socket.
  }

  private static class ConnectionHandler extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHandler.class);

    private final int consumerId;
    private final Socket socket;

    ConnectionHandler(int consumerId, Socket socket) {
      super("ConnectionHandler " + consumerId);
      this.consumerId = consumerId;
      this.socket = socket;
    }

    @Override
    public void run() {
      try {
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        final byte[] opCode = new byte[4]; // 4 == OPCODE length

        while (!Thread.currentThread().isInterrupted()) {
          inputStream.readFully(opCode);

          switch (Ints.fromByteArray(opCode)) {
            case Constants.Common.OPCODE_WRITE_INT:
              handleWrite(inputStream);
              break;
            default:
              throw new UnsupportedOperationException("Unknown opCode: " + Ints.fromByteArray(opCode) + " in Consumer: " + consumerId);
          }
        }
      } catch (EOFException | SocketException e) {
        LOGGER.info("Shutting down connection handler in Consumer: {} due to connection close.", consumerId);
      } catch (Exception e) {
        LOGGER.info("Shutting down connection handler in Consumer: {}", consumerId, e);
      } finally {
        try {
          socket.close();
        } catch (Exception e) {
          LOGGER.info("Error during ConnectionHandler shutdown in Consumer: {}", consumerId, e);
        }
      }
    }

    private void handleWrite(DataInputStream inputStream) throws IOException {
      long offset = inputStream.readLong();
      LOGGER.trace("Received write request for offset: {} in Consumer: {}", offset, consumerId);

      int keyLength = inputStream.readInt();
      byte[] key = new byte[keyLength];
      inputStream.readFully(key);
      int messageLength = inputStream.readInt();
      byte[] message = new byte[messageLength];
      inputStream.readFully(message);
      // TODO put in BEM
    }
  }
}
