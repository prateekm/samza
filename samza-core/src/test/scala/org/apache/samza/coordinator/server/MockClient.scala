package org.apache.samza.coordinator.server

import java.net.URL
import java.util.concurrent.Executors

import org.apache.samza.util.{ExponentialSleepStrategy, HttpUtil}

import scala.util.Random

object MockClient {
  def main(args: Array[String]): Unit = {
    val numClients = 512
    val executorService = Executors.newFixedThreadPool(numClients)

    for (i <- 1 to numClients) {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val jobModelUrl = "http://pmaheshw-mn2.linkedin.biz:58469/"
          val defaultReadJobModelDelay = 100
          val initialDelayMs = Random.nextInt(defaultReadJobModelDelay) + 1

          println(s"Client $i starting reading job model")
          HttpUtil.read(
            url = new URL(jobModelUrl),
            retryBackoff = new ExponentialSleepStrategy(initialDelayMs = initialDelayMs))
          println(s"Client $i finished reading job model")
        }
      })
    }
  }
}
