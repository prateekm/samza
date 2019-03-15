package org.apache.samza.system.p2p;

import com.google.common.util.concurrent.Uninterruptibles;

import java.time.Duration;
import java.util.Random;
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

    simulateFailures();

    System.exit(1);
  }

  private static void simulateFailures() {
    LOGGER.info("Starting orchestration.");
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
              LOGGER.info("Starting container " + id + " with runtime " + runtimeInSeconds);
              new ProcessExecutor().command((getCmd(id)).split("\\s+")) // args must be provided separately from cmd
                  .redirectOutput(Slf4jStream.of("Container " + id).asInfo())
                  .timeout(runtimeInSeconds, TimeUnit.SECONDS) // random timeout for force kill, must be > 0
                  .destroyOnExit()
                  .execute();
            } catch (TimeoutException te) {
              LOGGER.info("Timeout for container " + id);
            } catch (InterruptedException ie) {
              LOGGER.info("Interrupted for container " + id);
              break;
            } catch (Exception e) {
              LOGGER.error("Unexpected error for container " + id, e);
            }
            Uninterruptibles.sleepUninterruptibly(Constants.INTERVAL_BETWEEN_RESTART_SECONDS, TimeUnit.SECONDS);
          }
          LOGGER.info("Shutting down launcher thread for container " + id);
        } catch (Exception e) {
          LOGGER.error("Error in launcher thread for container " + id);
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
    return "/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/bin/java -Dfile.encoding=UTF-8 -classpath /Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/charsets.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/deploy.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/cldrdata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/dnsns.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/jaccess.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/jfxrt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/localedata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/nashorn.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/sunec.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/sunpkcs11.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/zipfs.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/javaws.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jce.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jfr.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jfxswt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jsse.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/management-agent.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/plugin.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/resources.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/rt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/ant-javafx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/dt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/javafx-mx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/jconsole.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/packager.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/sa-jdi.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/tools.jar:/Users/pmaheshw/code/work/prateekm-samza/out/test/samza-p2p:/Users/pmaheshw/code/work/prateekm-samza/out/production/samza-p2p:/Users/pmaheshw/code/work/prateekm-samza/out/test/samza-api:/Users/pmaheshw/code/work/prateekm-samza/out/production/samza-api:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-lang3/3.4/5fe28b9518e58819180a43a850fbc0dd24b7c050/commons-lang3-3.4.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.codehaus.jackson/jackson-mapper-asl/1.9.13/1ee2f2bed0e5dd29d1cb155a166e6f8d50bbddb7/jackson-mapper-asl-1.9.13.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.guava/guava/23.0/c947004bb13d18182be60077ade044099e4f26f1/guava-23.0.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-api/1.7.7/2b8019b6249bb05d81d3a3094e468753e2b21311/slf4j-api-1.7.7.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-core/3.1.2/224f03afd2521c6c94632f566beb1bb5ee32cf07/metrics-core-3.1.2.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.codehaus.jackson/jackson-core-asl/1.9.13/3c304d70f42f832e0a86d45bd437f692129299a4/jackson-core-asl-1.9.13.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.code.findbugs/jsr305/1.3.9/40719ea6961c0cb6afaeb6a921eaa1f6afd4cfdf/jsr305-1.3.9.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.errorprone/error_prone_annotations/2.0.18/5f65affce1684999e2f4024983835efc3504012e/error_prone_annotations-2.0.18.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.j2objc/j2objc-annotations/1.1/ed28ded51a8b1c6b112568def5f4b455e6809019/j2objc-annotations-1.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.codehaus.mojo/animal-sniffer-annotations/1.14/775b7e22fb10026eed3f86e8dc556dfafe35f2d5/animal-sniffer-annotations-1.14.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/junit/junit/4.12/2973d150c0dc1fefe998f834810d68f278ea58ec/junit-4.12.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.mockito/mockito-core/1.10.19/e8546f5bef4e061d8dd73895b4e8f40e3fe6effe/mockito-core-1.10.19.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.hamcrest/hamcrest-core/1.3/42a25dc3219429f0e5d060061f71acb49bf010a0/hamcrest-core-1.3.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.objenesis/objenesis/2.1/87c0ea803b69252868d09308b4618f766f135a96/objenesis-2.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.netty/netty-all/4.1.33.Final/c183bf3af840b72824d76a9f3324bf349da67788/netty-all-4.1.33.Final.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.rocksdb/rocksdbjni/5.7.3/421b44ad957a2b6cce5adedc204db551831b553d/rocksdbjni-5.7.3.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.zeroturnaround/zt-exec/1.10/6bec7a4af16208c7542d2c280207871c7b976483/zt-exec-1.10.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/commons-io/commons-io/1.4/a8762d07e76cfde2395257a5da47ba7c1dbd3dce/commons-io-1.4.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-simple/1.7.7/8095d0b9f7e0a9cd79a663c740e0f8fb31d0e2c8/slf4j-simple-1.7.7.jar org.apache.samza.system.p2p.Container " + containerId;
  }
}
