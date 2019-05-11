package org.apache.samza.system.p2p;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.system.p2p.pq.PersistentQueue;
import org.apache.samza.system.p2p.pq.PersistentQueueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProducerConnectionHandler extends SimpleChannelInboundHandler<Object> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConnectionHandler.class);
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4, // todo configure. use epoll event loop group?
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("P2P Data Sender Thread-%d").build());

  private final int producerId;
  private final int consumerId;
  private final PersistentQueue persistentQueue;
  private final Lock writeLock;
  private Channel channel;

  private volatile int numMessagesSent = 0; // TODO add gauge, THREAD SAFE?
  private volatile PersistentQueueIterator currentIterator; // TODO THREAD SAFE?
  private volatile ProducerOffset lastSentOffset = ProducerOffset.MIN_VALUE; // TODO THREAD SAFE?

  ProducerConnectionHandler(int producerId, int consumerId, PersistentQueue persistentQueue, Lock writeLock, Channel channel) {
    this.producerId = producerId;
    this.consumerId = consumerId;
    this.persistentQueue = persistentQueue;
    this.writeLock = writeLock;
    this.channel = channel; // TODO only safe if this is first outgoing handler in chain

    LOGGER.debug("SENDING HANDSHAKE {}", channel.toString());
    // Synchronize with the consumer on connection establishment. Send producerId to consumer to identify self.
    ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
    buffer.putInt(Constants.OPCODE_SYNC);
    buffer.putInt(producerId);
    channel.writeAndFlush(buffer.array()).addListener(f -> {
      if (f.isSuccess()) {
        // Start sending messages
        sendData(); // must not use context
      } else {
        LOGGER.error("Error sending sync message to Consumer: {} in Producer: {}. Closing channel.", consumerId, producerId);
        channel.close();
      }
    });
  }

  private void sendData() {
    LOGGER.debug("SEND DATA {}", channel.toString());
    if (hasData() && channel.isActive() && channel.isWritable()) {
      LOGGER.debug("SEND DATA TRUE {}", channel.toString());
      CompletableFuture
          .supplyAsync(this::getData, EXECUTOR) // fetch data and send it on a different thread
          .thenApply(data -> writeData(data)
              .addListener(future -> { // todo excessive context / syscall overhead in doing this per message? (runs on netty thread, requries flush to complete future)
                if (!future.isSuccess()) { // todo maybe not necessary to handle error here?
                  LOGGER.error("Error in connection to Consumer: {} in Producer: {}. Closing channel.",
                      consumerId, producerId, future.cause());
                  channel.close();
                }
              }))
          .thenAccept(f -> sendData()); // re-queues next send if still writable
    } else {
      // heartbeat to detect disconnects (TODO reduce frequency, check if necessary)
      // note: only sent if no data to send
      writeData(Ints.toByteArray(Constants.OPCODE_HEARTBEAT)); // TODO need sync? causes blocking operation exception
      // retry sending again later // TODO schedule on the event loop? http://normanmaurer.me/presentations/2014-facebook-eng-netty/slides.html#24.0
      P2PSystemProducer.EVENT_LOOP_GROUP.schedule(this::sendData, Constants.PRODUCER_CH_SEND_INTERVAL, TimeUnit.MILLISECONDS);
    }
  }

  private boolean hasData() {
    LOGGER.debug("HAS DATA {}", channel.toString());
    if (this.currentIterator != null && this.currentIterator.hasNext()) {
      return true;
    } else { // maybe reached end of previous iterator, recreate
      ProducerOffset startingOffset = this.lastSentOffset.nextOffset();
      LOGGER.debug("Next starting offset: {} for Producer: {}", startingOffset, producerId);
      if (this.currentIterator != null)  {
        this.currentIterator.close();
      }
      try {
        writeLock.lock();
        this.currentIterator = this.persistentQueue.readFrom(startingOffset);
      } finally {
        writeLock.unlock();
      }
      return this.currentIterator.hasNext();
    }
  }

  private byte[] getData() {
    LOGGER.debug("GET DATA {}", channel.toString());
    Pair<byte[], byte[]> entry = this.currentIterator.next();
    byte[] storedOffset = entry.getKey();
    byte[] payload = entry.getValue();
    this.numMessagesSent++;
    ProducerOffset producerOffset = new ProducerOffset(storedOffset);
    if (this.numMessagesSent % 1000 == 0) {
      LOGGER.debug("Sending data for offset: {} to Consumer: {} from Producer: {}",
          producerOffset, consumerId, producerId);
    } else {
      LOGGER.trace("Sending data for offset: {} to Consumer: {} from Producer: {}",
          producerOffset, consumerId, producerId);
    }
    this.lastSentOffset = producerOffset;

    ByteBuffer buffer = ByteBuffer.allocate(4 + storedOffset.length + payload.length);// TODO use composite buf
    buffer.putInt(Constants.OPCODE_WRITE);
    buffer.put(storedOffset);
    buffer.put(payload);
    return buffer.array();
  }

  private ChannelFuture writeData(byte[] data) {
    LOGGER.trace("WRITE DATA {}", channel.toString());
    // todo flush not necessary every time, batch flushes?
    return channel.writeAndFlush(data);
  }

  @Override
  public void channelActive(ChannelHandlerContext context) {
    LOGGER.debug("CHANNEL ACTIVE {}", channel.toString());
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    LOGGER.debug("CHANNEL INACTIVE {}", channel.toString());
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Object msg) {
    LOGGER.debug("CHANNEL READ UNEXPECTED {} {}", channel.toString(), msg);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.error("Error in connection to Consumer: {} in Producer: {}. Closing channel.",
        consumerId, producerId, cause);
    ctx.close();
  }

}
