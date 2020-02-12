package org.splink.cpipe.util

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.Promise

object FuturesExtended {
  implicit class ExtendedListableFuture[T](f: ListenableFuture[T]) {
    def asScala = {
      val promise = Promise[T]()
      val callback = new FutureCallback[T] {
        override def onSuccess(result: T): Unit = promise.success(result)

        override def onFailure(t: Throwable): Unit = promise.failure(t)
      }
      Futures.addCallback(f, callback)
      promise.future
    }
  }
}