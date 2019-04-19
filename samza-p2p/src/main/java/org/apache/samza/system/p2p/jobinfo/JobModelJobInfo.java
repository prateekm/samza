package org.apache.samza.system.p2p.jobinfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobModelJobInfo implements JobInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobModelJobInfo.class);

  private final JobModel jobModel;

  public JobModelJobInfo() {
    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
    LOGGER.info(String.format("Got coordinator URL: %s", coordinatorUrl));
    System.out.println(String.format("Coordinator URL: %s", coordinatorUrl));

    int delay = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
    jobModel = SamzaContainer.readJobModel(coordinatorUrl, delay);
  }

  @Override
  public int getNumPartitions() {
    return jobModel.getContainers().size();
  }

  @Override
  public List<TaskName> getAllTasks() {
    List<TaskName> tasks = new ArrayList<>();
    jobModel.getContainers().forEach((cid, cm) -> tasks.addAll(cm.getTasks().keySet()));
    return tasks;
  }
}
