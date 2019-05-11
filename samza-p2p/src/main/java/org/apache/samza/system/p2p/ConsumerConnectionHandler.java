package org.apache.samza.system.p2p;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumerConnectionHandler extends SimpleChannelInboundHandler<Object> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerConnectionHandler.class);

  private final int consumerId;
  private final AtomicReferenceArray<ProducerOffset> producerOffsets;
  private final P2PSystemConsumer.MessageSink messageSink;

  private int producerId = -1; // set after handshake / sync
  private int numMessagesReceived = 0;

  ConsumerConnectionHandler(int consumerId, AtomicReferenceArray<ProducerOffset> producerOffsets,
      P2PSystemConsumer.MessageSink messageSink) {
    this.consumerId = consumerId;
    this.producerOffsets = producerOffsets;
    this.messageSink = messageSink;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Object msg) {
    try {
      ByteBuffer message = ByteBuffer.wrap((byte[]) msg);
      int opCode = message.getInt();

      switch (opCode) {
        case Constants.OPCODE_SYNC:
          handleSync(message);
          break;
        case Constants.OPCODE_WRITE:
          handleWrite(message);
          break;
        case Constants.OPCODE_HEARTBEAT:
          handleHeartbeat();
          break;
        default:
          throw new UnsupportedOperationException("Unknown opCode: " + opCode
              + " in Consumer: " + consumerId + " for Producer: " + producerId);
      }
    } catch (Exception e) {
      LOGGER.info("Error in connection handler in Consumer: {} for Producer: {}", consumerId, producerId, e);
      throw new SamzaException(e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.info("Error in connection handler in Consumer: {} for Producer: {}", consumerId, producerId, cause);
    throw new SamzaException(cause);
  }

  private void handleSync(ByteBuffer contents) {
    this.producerId = contents.getInt();
  }

  private void handleWrite(ByteBuffer message) throws InterruptedException {
    byte[] producerOffsetBytes = new byte[ProducerOffset.NUM_BYTES];
    message.get(producerOffsetBytes);

    ProducerOffset producerOffset = new ProducerOffset(producerOffsetBytes);
    if (numMessagesReceived % 1000 == 0) {
      LOGGER.debug("Received write request from producer: {} with offset: {} in Consumer: {}",
          producerId, producerOffset, consumerId);
    } else {
      LOGGER.trace("Received write request from producer: {} with offset: {} in Consumer: {}",
          producerId, producerOffset, consumerId);
    }

    int systemLength = message.getInt();
    byte[] systemBytes = new byte[systemLength];
    message.get(systemBytes);

    int streamLength = message.getInt();
    byte[] streamBytes = new byte[streamLength];
    message.get(streamBytes);

    int partition = message.getInt();
    String systemName = new String(systemBytes);
    String streamName = new String(streamBytes);
    SystemStreamPartition ssp = new SystemStreamPartition(systemName, streamName, new Partition(partition));

    if (numMessagesReceived % 1000 == 0) {
      LOGGER.debug("Received write request for ssp: {} in Consumer: {}", ssp, consumerId);
    } else {
      LOGGER.trace("Received write request for ssp: {} in Consumer: {}", ssp, consumerId);
    }

    int keyLength = message.getInt();
    byte[] keyBytes = new byte[keyLength];
    message.get(keyBytes);

    int messageLength = message.getInt();
    byte[] messageBytes = new byte[messageLength];
    message.get(messageBytes);

    producerOffsets.set(producerId, producerOffset);
    String sspOffset = producerOffsets.toString(); // approx / non atomic OK.
    IncomingMessageEnvelope ime = new IncomingMessageEnvelope(ssp, sspOffset, keyBytes, messageBytes);

    try {
      numMessagesReceived++;
      messageSink.put(ssp, ime);
    } catch (Exception e) {
      LOGGER.error("Error putting IME: {} for SSP: {} in BEM", ime, ssp);
      throw e;
    }
  }

  private void handleHeartbeat() {
    LOGGER.trace("Received heartbeat request from producer: {} in Consumer: {}", producerId, consumerId);
  }
}
