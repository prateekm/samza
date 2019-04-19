package org.apache.samza.system.p2p.jobinfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.Constants;
import org.apache.samza.system.p2p.Util;
import org.apache.samza.system.p2p.jobinfo.JobInfo;

public class MCMTJobInfo implements JobInfo {
  @Override
  public int getNumContainers() {
    return Constants.NUM_CONTAINERS;
  }

  @Override
  public int getNumPartitions() {
    return Constants.NUM_PARTITIONS;
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
    return getAllTasks().stream().filter(tn -> {
      Integer partition = Integer.valueOf(tn.getTaskName().split("\\s")[2]);
      return (partition % Constants.NUM_CONTAINERS) == containerId;
    }).collect(Collectors.toList());
  }

  @Override
  public Set<SystemStreamPartition> getSSPsFor(int containerId) {
    Set<SystemStreamPartition> ssps = new HashSet<>();
    for (int i = 0; i < Constants.NUM_PARTITIONS; i++) {
      if ((i % Constants.NUM_CONTAINERS) == containerId) {
        ssps.add(new SystemStreamPartition(Constants.SYSTEM_NAME, Constants.STREAM_NAME, new Partition(i)));
      }
    }
    return ssps;
  }
}
