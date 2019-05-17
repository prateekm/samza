package org.apache.samza.system.p2p;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.flush.FlushConsolidationHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.p2p.jobinfo.ConsumerLocalityManager;
import org.apache.samza.system.p2p.pq.PersistentQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerConnectionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConnectionManager.class);
  private static EventLoopGroup EVENT_LOOP_GROUP; // created during bootstrap

  private final int numConsumers;
  private final int producerId;
  private final ConsumerLocalityManager consumerLocalityManager;
  private final ConcurrentMap<String, PersistentQueue> persistentQueues;
  private final ConcurrentMap<String, ReadWriteLock> consumerOffsetAndPQReadWriteLocks;
  private final Config config;
  private final MetricsRegistry metricsRegistry;
  private final Bootstrap bootstrap;

  private volatile boolean shutdown = false;

  public ProducerConnectionManager(int numConsumers, int producerId, ConsumerLocalityManager consumerLocalityManager,
      ConcurrentMap<String, PersistentQueue> persistentQueues,
      ConcurrentMap<String, ReadWriteLock> consumerOffsetAndPQReadWriteLocks,
      Config config, MetricsRegistry metricsRegistry) {
    this.numConsumers = numConsumers;
    this.producerId = producerId;
    this.consumerLocalityManager = consumerLocalityManager;
    this.persistentQueues = persistentQueues;
    this.consumerOffsetAndPQReadWriteLocks = consumerOffsetAndPQReadWriteLocks;
    this.config = config;
    this.metricsRegistry = metricsRegistry;
    this.bootstrap = this.createBootstrap();
  }

  public void start() {
    initConsumerConnections();
  }

  public void stop() {
    shutdown = true;
    if (EVENT_LOOP_GROUP != null) {
     EVENT_LOOP_GROUP.shutdownGracefully(0, 1000, TimeUnit.MILLISECONDS);
    }
  }

  private Bootstrap createBootstrap() {
    int flushConsolidationCount = config.getInt(Constants.P2P_PRODUCER_FLUSH_CONSOLIDATION_COUNT_CONFIG_KEY, 100);
    int lowWatermark = config.getInt(Constants.P2P_PRODUCER_WRITE_BUFFER_LOW_WATERMARK_BYTES_CONFIG_KEY, 32 * 1024);
    int highWatermark = config.getInt(Constants.P2P_PRODUCER_WRITE_BUFFER_HIGH_WATERMARK_BYTES_CONFIG_KEY, 64 * 1024);

    String channelType = config.get(Constants.P2P_PRODUCER_NETTY_CHANNEL_TYPE_CONFIG_KEY, "nio");
    ThreadFactory tf = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("P2P Producer Netty ELG Thread-%d").build();
    int numELGThreads = config.getInt(Constants.P2P_PRODUCER_ELG_THREADS_CONFIG_KEY, 4);
    Class<? extends SocketChannel> channelClass = null;
    if (channelType.equals("nio")) {
      EVENT_LOOP_GROUP = new NioEventLoopGroup(numELGThreads, tf);
      channelClass = NioSocketChannel.class;
    } else if (channelType.equals("epoll")){
      EVENT_LOOP_GROUP = new EpollEventLoopGroup(numELGThreads, tf);
      channelClass = EpollSocketChannel.class;
    }

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(EVENT_LOOP_GROUP)
        // TODO use local channel for local consumer produce
        .channel(channelClass)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            LOGGER.info("New channel initialized: {}", ch);
            ch.pipeline()
                .addLast("flushConsolidationHandler", new FlushConsolidationHandler(flushConsolidationCount, true)) // todo tune, stable? can block indefinitely if no new write?
                .addLast("snappyCompressor", new SnappyFrameEncoder()) // TODO do not need compression per message. batch?
                .addLast("lengthFieldPrepender", new LengthFieldPrepender(4))
                .addLast("byteArrayEncoder", new ByteArrayEncoder());
            // MessageAggregator
            // Read/WriteTimeoutHandler?
            // IdleStateHandler
            // https://github.com/spotify/netty-batch-flusher
          }
        })
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(lowWatermark, highWatermark));
    // todo bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)?
    // http://normanmaurer.me/presentations/2014-facebook-eng-netty/slides.html#15.0
    return bootstrap;
  }

  public void initConsumerConnections() {
    try {
      for (int consumerId = 0; consumerId < numConsumers; consumerId++) {
        doConnect(consumerId, persistentQueues.get(String.valueOf(consumerId)));
      }
    } catch (Exception e) {
      throw new SamzaException("Unable to initiate consumer connections", e);
    }
  }

  private void doConnect(int consumerId, PersistentQueue persistentQueue) {
    // read the consumer port from metadata store every time.
    InetSocketAddress consumerAddress = consumerLocalityManager.getConsumerAddress(String.valueOf(consumerId));
    LOGGER.info("Connecting at address: {} for Consumer: {} in Producer: {}", consumerAddress, consumerId, producerId);

    ChannelFuture cf = bootstrap.connect(consumerAddress);

    cf.addListener((ChannelFuture f) -> {
      if (f.isSuccess()) {
        LOGGER.info("Connected to Consumer: {} in Producer: {}.", consumerId, producerId);
        Lock writeLock = consumerOffsetAndPQReadWriteLocks.get(String.valueOf(consumerId)).writeLock();
        cf.channel().pipeline()
            .addLast(new ProducerConnectionHandler(producerId, consumerId, persistentQueue, writeLock, cf.channel()));
      }
    });

    cf.channel().closeFuture().addListener(f -> { // auto reconnect on stop
      // TODO assumes always stop connection cleanly on error?
      if (!shutdown) {
        LOGGER.error("Channel closed to Consumer: {} in Producer: {}. Retrying", consumerId, producerId, f.cause());
        cf.channel().eventLoop().schedule(() -> doConnect(consumerId, persistentQueue),
            Constants.PRODUCER_CH_CONNECTION_RETRY_INTERVAL, TimeUnit.MILLISECONDS);
      }
    });
  }
}
