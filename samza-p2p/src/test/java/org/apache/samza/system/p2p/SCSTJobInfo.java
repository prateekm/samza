package org.apache.samza.system.p2p;

import com.google.common.collect.ImmutableList;

import java.util.List;
import org.apache.samza.container.TaskName;

public class SCSTJobInfo implements JobInfo {
  @Override
  public int getNumPartitions() {
    return 1;
  }

  @Override
  public int getPartitionFor(byte[] key, int numPartitions) {
    return 0;
  }

  @Override
  public int getConsumerFor(int partition) {
    return 0;
  }

  @Override
  public List<TaskName> getTasks() {
    return ImmutableList.of(new TaskName("Partition 0"));
  }
}
