package org.apache.samza.storage

import java.io.File

import org.apache.samza.Partition
import org.apache.samza.config.{Config, TaskConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.job.model.TaskMode
import org.apache.samza.system.{SystemAdmins, SystemStream}

object TaskStorageManagerFactory {
  def create(taskName: TaskName, containerStorageManager: ContainerStorageManager,
             storeChangelogs: Map[String, SystemStream], systemAdmins: SystemAdmins,
             loggedStoreBaseDir: File, changelogPartition: Partition,
             config: Config, taskMode: TaskMode): TaskStorageManager = {
    if (new TaskConfig(config).getTransactionalStateEnabled()) {
      throw new UnsupportedOperationException
    } else {
      new NonTransactionalStateTaskStorageManager(taskName, containerStorageManager, storeChangelogs, systemAdmins,
        loggedStoreBaseDir, changelogPartition)
    }
  }
}
