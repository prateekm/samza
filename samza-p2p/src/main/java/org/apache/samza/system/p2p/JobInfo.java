package org.apache.samza.system.p2p;

import java.util.List;
import org.apache.samza.container.TaskName;

public interface JobInfo {
  int getNumPartitions();
  int getPartitionFor(byte[] key, int numPartitions);
  int getConsumerFor(int partition);
  List<TaskName> getTasks();
}

