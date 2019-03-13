package org.apache.samza.system.p2p;

import com.google.common.util.concurrent.Uninterruptibles;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

public class Orchestrator {
  static {
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "false");
    System.setProperty(SimpleLogger.SHOW_THREAD_NAME_KEY, "false");
    System.setProperty(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, "true");
    System.setProperty(SimpleLogger.LEVEL_IN_BRACKETS_KEY, "true");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Orchestrator.class);
  private static final Random RANDOM = new Random();

  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
      LOGGER.error("Uncaught error in thread: " + t, e);
      System.exit(1);
    });

    simulateFailuresWithHostAffinity();
  }

  private static void simulateFailuresWithHostAffinity() {
    LOGGER.info("Simulating Failures with Host Affinity");
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    long finishTimeMs = System.currentTimeMillis() + Duration.ofSeconds(Constants.TOTAL_RUNTIME_SECONDS).toMillis() / 2;

    for (int i = 0; i < Constants.NUM_CONTAINERS; i++) {
      final int id = i;
      executorService.submit(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            try {
              int remainingTimeInSeconds =  Math.max((int) (finishTimeMs - System.currentTimeMillis()) / 1000, 0);
              int runtimeInSeconds = Math.min((Constants.MIN_RUNTIME_SECONDS +
                  RANDOM.nextInt(Constants.MAX_RUNTIME_SECONDS -
                      Constants.MIN_RUNTIME_SECONDS)), remainingTimeInSeconds) + 1;
              if (runtimeInSeconds <= 0) throw new RuntimeException();
              LOGGER.info("Starting process " + id + " with runtime " + runtimeInSeconds);
              new ProcessExecutor().command((getCmd(id)).split("\\s+")) // args must be provided separately from cmd
                  .redirectOutput(Slf4jStream.of("Container " + id).asInfo())
                  .timeout(runtimeInSeconds, TimeUnit.SECONDS) // random timeout for force kill, must be > 0
                  .destroyOnExit()
                  .execute();
            } catch (TimeoutException te) {
              LOGGER.info("Timeout for process " + id);
            } catch (InterruptedException ie) {
              LOGGER.info("Interrupted for process " + id);
              break;
            } catch (Exception e) {
              LOGGER.error("Unexpected error for process " + id, e);
            }
            Uninterruptibles.sleepUninterruptibly(Constants.INTERVAL_BETWEEN_RESTART_SECONDS, TimeUnit.SECONDS);
          }
          LOGGER.info("Shutting down launcher thread for process " + id);
        } catch (Exception e) {
          LOGGER.error("Error in launcher thread for process " + id);
        }
      });
    }
    Uninterruptibles.sleepUninterruptibly(Constants.TOTAL_RUNTIME_SECONDS / 2, TimeUnit.SECONDS);
    LOGGER.info("Shutting down executor service.");
    executorService.shutdownNow();
    Uninterruptibles.sleepUninterruptibly(Constants.MAX_RUNTIME_SECONDS, TimeUnit.SECONDS); // let running processes die.
    LOGGER.info("Shut down process execution.");
  }

  private static String getCmd(int containerId) {
    return "java -Dfile.encoding=UTF-8 -classpath build/libs/c2c-replication-0.1.jar system.Container " + containerId;
  }
}
