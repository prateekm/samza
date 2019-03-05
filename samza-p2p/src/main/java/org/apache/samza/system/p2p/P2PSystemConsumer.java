package org.apache.samza.system.p2p;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;

public class P2PSystemConsumer implements SystemConsumer {

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {

  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> systemStreamPartitions, long timeout) {
    return Collections.emptyMap();
  }
}
