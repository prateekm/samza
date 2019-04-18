package org.apache.samza.system.p2p;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

public class SCSTJobInfo implements JobInfo {
  @Override
  public int getNumPartitions() {
    return 1;
  }

  @Override
  public int getPartitionFor(byte[] key) {
    return 0;
  }

  @Override
  public int getConsumerFor(int partition) {
    return 0;
  }

  @Override
  public List<TaskName> getAllTasks() {
    return ImmutableList.of(
        new TaskName("Source Partition 0"),
        new TaskName("Sink Partition 0"));
  }

  @Override
  public List<TaskName> getTasksFor(int containerId) {
    return getAllTasks();
  }

  @Override
  public Set<SystemStreamPartition> getSSPsFor(int containerId) {
    Set<SystemStreamPartition> ssps = new HashSet<>();
    for (int i = 0; i < Constants.NUM_PARTITIONS; i++) {
      ssps.add(new SystemStreamPartition(Constants.SYSTEM_NAME, Constants.STREAM_NAME, new Partition(i)));
    }
    return ssps;
  }
}
