package org.apache.samza.coordinator.server

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

import com.sun.management.UnixOperatingSystemMXBean
import org.apache.samza.job.model.JobModel
import org.apache.samza.serializers.model.SamzaObjectMapper

object MockServer {
  def main(args: Array[String]): Unit = {
    val fdCountThread = new Thread(new Runnable() {
      override def run(): Unit = {
        while (true) {
          val os = ManagementFactory.getOperatingSystemMXBean
          val numOpenFDs = os.asInstanceOf[UnixOperatingSystemMXBean].getOpenFileDescriptorCount()
          println(s"Num open fds: $numOpenFDs")
          Thread.sleep(1000)
        }
      }
    }, "FD Count Thread")
    fdCountThread.start()

    val jobModelPath = Paths.get("/Users/pmaheshw/code/work/prateekm-samza/samza-core/src/test/scala/org/apache/samza/coordinator/server/jobModel.txt")
    val jobModel = SamzaObjectMapper.getObjectMapper.readValue(
      new String(Files.readAllBytes(jobModelPath), StandardCharsets.UTF_8), classOf[JobModel])
    val jobModelRef = new AtomicReference[JobModel]()
    jobModelRef.set(jobModel)

    val server = new HttpServer("/", 58469)
    server.addServlet("/", new JobServlet(jobModelRef))
    server.start

    println(server.getUrl)
    Thread.sleep(Duration.ofMinutes(30).toMillis)
  }
}
