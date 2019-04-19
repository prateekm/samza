package org.apache.samza.system.p2p.jobinfo;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.p2p.Util;

public interface JobInfo {
  int getNumContainers();
  int getNumPartitions();
  List<TaskName> getAllTasks();

  default List<TaskName> getTasksFor(int containerId) { // TODO used in tests only
    return Collections.emptyList();
  }

  default Set<SystemStreamPartition> getSSPsFor(int containerId) { // TODO used in tests only
    return Collections.emptySet();
  }

  default int getPartitionFor(byte[] key) {
    return Util.toPositive(Util.murmur2(key)) % getNumPartitions();
  }

  default int getConsumerFor(int partition) {
    return partition % getNumContainers(); // todo should be % num containers, not num tasks
  }
}

