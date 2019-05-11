/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.system.p2p;

import com.google.common.util.concurrent.Uninterruptibles;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
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

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
        LOGGER.error("Uncaught error in thread: " + t, e);
        System.exit(1);
      });

    simulateFailures();

    System.exit(1);
  }

  private static void simulateFailures() throws IOException {
    LOGGER.info("Starting orchestration.");
    FileUtils.deleteDirectory(new File(Constants.Test.SHARED_STATE_BASE_PATH));

    ExecutorService executorService = Executors.newFixedThreadPool(3);
    long finishTimeMs = System.currentTimeMillis() + Duration.ofSeconds(Constants.Test.TOTAL_RUNTIME_SECONDS).toMillis() / 2;

    for (int i = 0; i < Constants.Test.NUM_CONTAINERS; i++) {
      final int id = i;
      executorService.submit(() -> {
          try {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                int remainingTimeInSeconds =  Math.max((int) (finishTimeMs - System.currentTimeMillis()) / 1000, 0);
                int runtimeInSeconds = Math.min(Constants.Test.MIN_RUNTIME_SECONDS +
                    RANDOM.nextInt(Constants.Test.MAX_RUNTIME_SECONDS -
                        Constants.Test.MIN_RUNTIME_SECONDS), remainingTimeInSeconds) + 1;
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
              Uninterruptibles.sleepUninterruptibly(Constants.Test.INTERVAL_BETWEEN_RESTART_SECONDS, TimeUnit.SECONDS);
            }
            LOGGER.info("Shutting down launcher thread for container " + id);
          } catch (Exception e) {
            LOGGER.error("Error in launcher thread for container " + id);
          }
        });
    }
    Uninterruptibles.sleepUninterruptibly(Constants.Test.TOTAL_RUNTIME_SECONDS / 2, TimeUnit.SECONDS);
    LOGGER.info("Shutting down executor service.");
    executorService.shutdownNow();
    Uninterruptibles.sleepUninterruptibly(Constants.Test.MAX_RUNTIME_SECONDS, TimeUnit.SECONDS); // let running processes die.
    LOGGER.info("Shut down process execution.");
  }

  private static String getCmd(int containerId) {
    return "/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/bin/java -classpath /Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/charsets.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/deploy.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/cldrdata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/dnsns.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/jaccess.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/jfxrt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/localedata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/nashorn.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/sunec.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/sunpkcs11.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/zipfs.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/javaws.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jce.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jfr.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jfxswt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jsse.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/management-agent.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/plugin.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/resources.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/rt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/ant-javafx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/dt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/javafx-mx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/jconsole.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/packager.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/sa-jdi.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/tools.jar:/Users/pmaheshw/code/work/prateekm-samza/out/test/samza-p2p_2.11:/Users/pmaheshw/code/work/prateekm-samza/out/production/samza-p2p_2.11:/Users/pmaheshw/code/work/prateekm-samza/out/test/samza-core_2.11:/Users/pmaheshw/code/work/prateekm-samza/out/production/samza-core_2.11:/Users/pmaheshw/code/work/prateekm-samza/out/test/samza-api:/Users/pmaheshw/code/work/prateekm-samza/out/production/samza-api:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-lang3/3.4/5fe28b9518e58819180a43a850fbc0dd24b7c050/commons-lang3-3.4.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.codehaus.jackson/jackson-mapper-asl/1.9.13/1ee2f2bed0e5dd29d1cb155a166e6f8d50bbddb7/jackson-mapper-asl-1.9.13.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.guava/guava/23.0/c947004bb13d18182be60077ade044099e4f26f1/guava-23.0.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.code.gson/gson/2.8.5/f645ed69d595b24d4cf8b3fbb64cc505bede8829/gson-2.8.5.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-core/3.1.2/224f03afd2521c6c94632f566beb1bb5ee32cf07/metrics-core-3.1.2.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-api/1.7.7/2b8019b6249bb05d81d3a3094e468753e2b21311/slf4j-api-1.7.7.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.codehaus.jackson/jackson-core-asl/1.9.13/3c304d70f42f832e0a86d45bd437f692129299a4/jackson-core-asl-1.9.13.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.code.findbugs/jsr305/1.3.9/40719ea6961c0cb6afaeb6a921eaa1f6afd4cfdf/jsr305-1.3.9.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.errorprone/error_prone_annotations/2.0.18/5f65affce1684999e2f4024983835efc3504012e/error_prone_annotations-2.0.18.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.j2objc/j2objc-annotations/1.1/ed28ded51a8b1c6b112568def5f4b455e6809019/j2objc-annotations-1.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.codehaus.mojo/animal-sniffer-annotations/1.14/775b7e22fb10026eed3f86e8dc556dfafe35f2d5/animal-sniffer-annotations-1.14.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/junit/junit/4.12/2973d150c0dc1fefe998f834810d68f278ea58ec/junit-4.12.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.mockito/mockito-core/1.10.19/e8546f5bef4e061d8dd73895b4e8f40e3fe6effe/mockito-core-1.10.19.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.hamcrest/hamcrest-core/1.3/42a25dc3219429f0e5d060061f71acb49bf010a0/hamcrest-core-1.3.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.objenesis/objenesis/2.1/87c0ea803b69252868d09308b4618f766f135a96/objenesis-2.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.101tec/zkclient/0.8/c0f700a4a3b386279d7d8dd164edecbe836cbfdb/zkclient-0.8.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/net.sf.jopt-simple/jopt-simple/3.2/d625f12ba08083c8c16dcedd5396ec730e9e77ab/jopt-simple-3.2.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-collections4/4.0/da217367fd25e88df52ba79e47658d4cf928b0d1/commons-collections4-4.0.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.eclipse.jetty/jetty-webapp/9.2.7.v20150116/c425e57423123c89ed72e76df8f5d0332f05ecee/jetty-webapp-9.2.7.v20150116.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.scala-lang/scala-library/2.11.8/ddd5a8bced249bedd86fb4578a39b9fb71480573/scala-library-2.11.8.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.apache.zookeeper/zookeeper/3.4.6/1b2502e29da1ebaade2357cd1de35a855fa3755/zookeeper-3.4.6.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.eclipse.jetty/jetty-xml/9.2.7.v20150116/81008da0080e82fd3c8e6d094a1241ce33ca7c6d/jetty-xml-9.2.7.v20150116.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.eclipse.jetty/jetty-servlet/9.2.7.v20150116/4c11b757c34627b21e493201a3173ed12ebe04a6/jetty-servlet-9.2.7.v20150116.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/jline/jline/0.9.94/99a18e9a44834afdebc467294e1138364c207402/jline-0.9.94.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.netty/netty/3.7.0.Final/7a8c35599c68c0bf383df74469aa3e03d9aca87/netty-3.7.0.Final.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.eclipse.jetty/jetty-security/9.2.7.v20150116/f3f257d952bbe493a6a187d95395464fce9926fe/jetty-security-9.2.7.v20150116.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.eclipse.jetty/jetty-server/9.2.7.v20150116/dc072b171e8b7ad5762c765c54bdf4e3b2198225/jetty-server-9.2.7.v20150116.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.eclipse.jetty/jetty-http/9.2.7.v20150116/ac6c19ab7daaffe86622f2d78e4cbeff1a674e85/jetty-http-9.2.7.v20150116.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.eclipse.jetty/jetty-io/9.2.7.v20150116/11bd06fca00dd0c27a25aa0ba46a7c2cf19527d5/jetty-io-9.2.7.v20150116.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.eclipse.jetty/jetty-util/9.2.7.v20150116/2fb9c6172e5704b1b6d8609c5e9dc15db3367d7/jetty-util-9.2.7.v20150116.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/junit/junit/3.8.1/99129f16442844f6a4a11ae22fbbee40b14d774f/junit-3.8.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/javax.servlet/javax.servlet-api/3.1.0/3cd63d075497751784b2fa84be59432f4905bf7c/javax.servlet-api-3.1.0.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.scalatest/scalatest_2.11/3.0.1/40a1842e7f0b915d87de1cb69f9c6962a65ee1fd/scalatest_2.11-3.0.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.scalactic/scalactic_2.11/3.0.1/3c444d143879dc172fa555cea08fd0de6fa2f34f/scalactic_2.11-3.0.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.scala-lang/scala-reflect/2.11.8/b74530deeba742ab4f3134de0c2da0edc49ca361/scala-reflect-2.11.8.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.scala-lang.modules/scala-xml_2.11/1.0.5/77ac9be4033768cf03cc04fbd1fc5e5711de2459/scala-xml_2.11-1.0.5.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.scala-lang.modules/scala-parser-combinators_2.11/1.0.4/7369d653bcfa95d321994660477a4d7e81d7f490/scala-parser-combinators_2.11-1.0.4.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-module-junit4/1.6.6/6dd79e43e666a017545c7e32fe10da127650df6e/powermock-module-junit4-1.6.6.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-module-junit4-common/1.6.6/6302c934d03f76fa348ec91c603e11ce05b61f44/powermock-module-junit4-common-1.6.6.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-api-mockito/1.6.6/99e7e9f0133ba5595b8a44cc6530baefda7e7e9e/powermock-api-mockito-1.6.6.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-api-mockito-common/1.6.6/da4b2a87c56506c49cef6e285724c266a719b63c/powermock-api-mockito-common-1.6.6.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-api-support/1.6.6/5ba4af06a0345c615efcdadb6ef35f5ae5a39a36/powermock-api-support-1.6.6.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-core/1.6.6/8085fae46f60d7ff960f1cc711359c00b35c5887/powermock-core-1.6.6.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.hamcrest/hamcrest-all/1.3/63a21ebc981131004ad02e0434e799fd7f3a8d5a/hamcrest-all-1.3.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-reflect/1.6.6/3fa5d0acee85c5662102ab2ef7a49bbb5a56bae5/powermock-reflect-1.6.6.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.objenesis/objenesis/2.4/2916b6c96b50c5b3ec4452ed99401db745aabb27/objenesis-2.4.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.javassist/javassist/3.21.0-GA/598244f595db5c5fb713731eddbb1c91a58d959b/javassist-3.21.0-GA.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.netty/netty-all/4.1.35.Final/e8b0228a383be6b1269e98e937502deb6a324de6/netty-all-4.1.35.Final.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.rocksdb/rocksdbjni/5.7.3/421b44ad957a2b6cce5adedc204db551831b553d/rocksdbjni-5.7.3.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.zeroturnaround/zt-exec/1.10/6bec7a4af16208c7542d2c280207871c7b976483/zt-exec-1.10.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/commons-io/commons-io/1.4/a8762d07e76cfde2395257a5da47ba7c1dbd3dce/commons-io-1.4.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-simple/1.7.7/8095d0b9f7e0a9cd79a663c740e0f8fb31d0e2c8/slf4j-simple-1.7.7.jar org.apache.samza.system.p2p.Container " + containerId;
  }
}
