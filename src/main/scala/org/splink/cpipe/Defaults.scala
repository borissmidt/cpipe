package org.splink.cpipe

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.{Executors, ThreadFactory}

import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.scalalogging.LazyLogging
import org.splink.cpipe.config.Settings

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Copyright (C) 06.02.20 - REstore NV
  */
object Defaults extends LazyLogging {
  //we could get this from a scala conf so it becomes configurable?
  val cassandraTimeoutMilis = 10.minutes.toMillis.toInt
  val cassandraPort = 9042

  val batchSize = 10
  val fetchSize = 5000

  implicit val ioPool = ExecutionContext.fromExecutor(
    Executors.newCachedThreadPool(
      ThreadFactoryBuilder(
        "io",
        reporter = new UncaughtExceptionHandler {
          override def uncaughtException(thread: Thread, throwable: Throwable): Unit = {
            logger.error(s"unhandled exception: ${thread.getName}", throwable)
          }
        },
        daemonic = true
      )
    )
  )

  implicit val settings = Settings(
    fetchSize,batchSize,ConsistencyLevel.ONE
  )

}

object ThreadFactoryBuilder {

  /** Constructs a ThreadFactory using the provided name prefix and appending
    * with a unique incrementing thread identifier.
    *
    * @param name     the created threads name prefix, for easy identification.
    * @param daemonic specifies whether the created threads should be daemonic
    *                 (non-daemonic threads are blocking the JVM process on exit).
    */
  def apply(
      name: String,
      reporter: Thread.UncaughtExceptionHandler,
      daemonic: Boolean
    ): ThreadFactory = {
    new ThreadFactory {
      def newThread(r: Runnable) = {
        val thread = new Thread(r)
        thread.setName(name + "-" + thread.getId)
        thread.setDaemon(daemonic)
        thread.setUncaughtExceptionHandler(reporter)
        thread
      }
    }
  }
}
