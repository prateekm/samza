package org.apache.samza.system.p2p.jobinfo;

import java.net.InetSocketAddress;
import org.apache.samza.system.p2p.Constants;
import org.apache.samza.system.p2p.Util;

public class FixedConsumerLocalityManager implements ConsumerLocalityManager {
  @Override
  public void start() {

  }

  @Override
  public InetSocketAddress getConsumerAddress(String containerId) {
    long port = Util.readFileLong(Constants.Test.getConsumerPortPath(containerId));
    return new InetSocketAddress("localhost", (int) port);
  }

  @Override
  public void writeConsumerPort(String containerId, Integer port) {
    try {
      Util.writeFile(Constants.Test.getConsumerPortPath(containerId), (long) port);
    } catch (Exception e) {
      throw new RuntimeException("Could not write port file for consumer: " + containerId, e);
    }
  }

  @Override
  public void stop() {

  }
}
