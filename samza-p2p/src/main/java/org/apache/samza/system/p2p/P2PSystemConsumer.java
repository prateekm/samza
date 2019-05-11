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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.compression.SnappyFrameDecoder;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
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
  private static final EventLoopGroup BOSS_GROUP = new NioEventLoopGroup(1,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("P2P Consumer Netty Boss ELG Thread-%d").build());
  private static final EventLoopGroup WORKER_GROUP = new NioEventLoopGroup(4, // todo configure. use epoll event loop group?
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("P2P Consumer Netty Worker ELG Thread-%d").build());

  private final int consumerId;
  private final ConsumerLocalityManager consumerLocalityManager;
  private final MessageSink messageSink;
  private final AtomicReferenceArray<ProducerOffset> producerOffsets;
  private final ServerBootstrap bootstrap;

  public P2PSystemConsumer(int consumerId, Config config, MetricsRegistry metricsRegistry, Clock clock,
      ConsumerLocalityManager consumerLocalityManager) {
    super(metricsRegistry, clock);
    this.consumerId = consumerId;
    this.consumerLocalityManager = consumerLocalityManager;
    this.messageSink = new MessageSink(this);

    int numProducers = config.getInt(JobConfig.JOB_CONTAINER_COUNT());
    this.producerOffsets = new AtomicReferenceArray<>(numProducers);
    for (int i = 0; i < numProducers; i++) {
      producerOffsets.set(i, ProducerOffset.MIN_VALUE);
    }

    this.bootstrap = createBootstrap(consumerId);
  }

  @Override
  public void start() {
    LOGGER.info("Starting P2PSystemConsumer: {}.", consumerId);
    try {
      // Bind and start to accept incoming connections.
      ChannelFuture cf = bootstrap.bind(0).sync();
      InetSocketAddress localAddress = (InetSocketAddress) cf.channel().localAddress();
      int consumerPort = localAddress.getPort();

      LOGGER.info("Writing Port: {} for Consumer: {}", consumerPort, consumerId);
      consumerLocalityManager.start();
      consumerLocalityManager.writeConsumerPort(String.valueOf(consumerId), consumerPort);
      consumerLocalityManager.stop();
    } catch (Exception e) {
      throw new SamzaException("Error starting server in Consumer: " + consumerId, e);
    }
    LOGGER.info("Started P2PSystemConsumer: {}.", consumerId);
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping P2PSystemConsumer: {}.", consumerId);
    WORKER_GROUP.shutdownGracefully(0, 1000, TimeUnit.MILLISECONDS);
    BOSS_GROUP.shutdownGracefully(0, 1000, TimeUnit.MILLISECONDS);
    LOGGER.info("Stopped P2PSystemConsumer: {}.", consumerId);
  }

  private ServerBootstrap createBootstrap(int consumerId) {
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(BOSS_GROUP, WORKER_GROUP)
        .channel(NioServerSocketChannel.class) // use epoll socket channel?
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            LOGGER.debug("New channel initialized: {}", ch);
            ch.pipeline()
                .addLast("snappyDecompressor", new SnappyFrameDecoder()) // todo only useful if batching, maybe disable until then.
                .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
                .addLast("byteArrayDecoder", new ByteArrayDecoder())
                .addLast(new ConsumerConnectionHandler(consumerId, producerOffsets, messageSink));
          }
        })
        .childOption(ChannelOption.SO_KEEPALIVE, true) // tcp keepalive, timeout at os level (2+ hours default)
        .childOption(ChannelOption.TCP_NODELAY, true) // disable nagle's algorithm
        .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000); // 30 sec == same as default
        // don't set soTimeout since producer may not send for a while (// todo still true after heartbeat? maybe user idlestatehandler instead?)
        // todo check if need any other options

    return bootstrap;
  }

  private static class ConsumerConnectionHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerConnectionHandler.class);

    private final int consumerId;
    private final AtomicReferenceArray<ProducerOffset> producerOffsets;
    private final MessageSink messageSink;

    private int producerId = -1; // set after handshake / sync
    private int numMessagesReceived = 0;

    ConsumerConnectionHandler(int consumerId, AtomicReferenceArray<ProducerOffset> producerOffsets, MessageSink messageSink) {
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
