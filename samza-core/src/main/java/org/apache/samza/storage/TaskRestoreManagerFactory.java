package org.apache.samza.storage;

import java.io.File;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Clock;

/**
 * Factory class to create {@link TaskRestoreManager}.
 */
class TaskRestoreManagerFactory {

  public static TaskRestoreManager create(
      TaskModel taskModel,
      Map<String, SystemStream> changelogSystemStreams,
      Map<String, StorageEngine> taskStores,
      SystemAdmins systemAdmins,
      StreamMetadataCache streamMetadataCache,
      SSPMetadataCache sspMetadataCache,
      Map<String, SystemConsumer> storeConsumers,
      int maxChangeLogStreamPartitions,
      File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory,
      Config config,
      Clock clock) {

    if (new TaskConfig(config).getTransactionalStateEnabled()) {
      // Create checkpoint-snapshot based state restoration which is transactional.
      return new TransactionalStateTaskRestoreManager(
          taskModel,
          taskStores,
          changelogSystemStreams,
          systemAdmins,
          storeConsumers,
          sspMetadataCache,
          loggedStoreBaseDirectory,
          nonLoggedStoreBaseDirectory,
          config,
          clock
      );
    } else {
      // Create legacy offset-file based state restoration which is NOT transactional.
      return new NonTransactionalStateTaskRestoreManager(
          taskModel,
          changelogSystemStreams,
          taskStores,
          systemAdmins,
          streamMetadataCache,
          storeConsumers,
          maxChangeLogStreamPartitions,
          loggedStoreBaseDirectory,
          nonLoggedStoreBaseDirectory,
          config,
          clock);
    }
  }
}
