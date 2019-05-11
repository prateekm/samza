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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.compression.SnappyFrameDecoder;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
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

  class MessageSink {
    private final P2PSystemConsumer consumer;

    MessageSink(P2PSystemConsumer consumer) {
      this.consumer = consumer;
    }

    void put(SystemStreamPartition ssp, IncomingMessageEnvelope ime) throws InterruptedException {
      consumer.put(ssp, ime);
    }
  }
}
