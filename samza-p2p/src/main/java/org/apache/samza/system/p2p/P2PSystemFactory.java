package org.apache.samza.system.p2p;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.p2p.checkpoint.KCMWatcherFactory;
import org.apache.samza.system.p2p.jobinfo.JobModelJobInfo;
import org.apache.samza.system.p2p.pq.RocksDBPersistentQueueFactory;
import org.apache.samza.util.Clock;

public class P2PSystemFactory implements SystemFactory {
  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    return new P2PSystemConsumer(0 /* TODO FIX */, registry, System::currentTimeMillis);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new P2PSystemProducer(systemName, 0 /* TODO FIX */, new RocksDBPersistentQueueFactory(), new KCMWatcherFactory(), config, registry, new JobModelJobInfo());
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new P2PSystemAdmin();
  }
}
