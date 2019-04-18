package org.apache.samza.system.p2p.jobinfo;

import java.util.List;
import java.util.Set;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

public interface JobInfo {
  int getNumPartitions();
  int getPartitionFor(byte[] key);
  int getConsumerFor(int partition);
  List<TaskName> getAllTasks();
  List<TaskName> getTasksFor(int containerId);
  Set<SystemStreamPartition> getSSPsFor(int containerId);
}

