package org.apache.samza.system.p2p.jobinfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.Constants;
import org.apache.samza.system.p2p.Util;
import org.apache.samza.system.p2p.jobinfo.JobInfo;

public class SCMTJobInfo implements JobInfo {
  @Override
  public int getNumPartitions() {
    return Constants.NUM_PARTITIONS;
  }

  @Override
  public int getPartitionFor(byte[] key) {
    return Util.toPositive(Util.murmur2(key)) % getNumPartitions();
  }

  @Override
  public int getConsumerFor(int partition) {
    return 0;
  }

  @Override
  public List<TaskName> getAllTasks() {
    ArrayList<TaskName> taskNames = new ArrayList<>();
    for (int i = 0; i < getNumPartitions(); i++) {
      taskNames.add(new TaskName("Source Partition " + i));
      taskNames.add(new TaskName("Sink Partition " + i));
    }
    return taskNames;
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
