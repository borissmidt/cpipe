package org.splink.cpipe.processors
import java.util.concurrent.{Executor, Executors}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.utils.MoreFutures
import com.datastax.driver.core.{ArrayBackedRow, PreparedStatement, RegularStatement, Row, Session, SimpleStatement, Statement}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import com.typesafe.scalalogging.LazyLogging
import org.rogach.scallop.ArgType.V
import org.splink.cpipe.Cassandra.CassandraHelper
import org.splink.cpipe.Rps
import org.splink.cpipe.config.{Config, Defaults, Selection}

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import FuturesExtended._

import scala.util.{Failure, Success}

/**
  * Copyright (C) 06.02.20 - REstore NV
  */
case class Transporter(from: Session, to: Session) extends LazyLogging {

  def selectionToReadQuery(selection: Selection) = {
    import selection._
    val tableName = s"$keyspace.$table"
    val where = if (filter.isEmpty) {
      ""
    } else {
      s"where $filter"
    }

    val query = s"select * from $tableName" + where
    new SimpleStatement(query)
  }

  case class Reporter(
      count: AtomicInteger = new AtomicInteger(0),
      timestamp: AtomicLong = new AtomicLong(0),
      printEvery: Int = 5000,
      batchSize: Int = 1
    ) {
    def poke() = {
      val currentBatch = count.incrementAndGet()
      if (currentBatch % (printEvery / batchSize) == 0) {
        val now = System.nanoTime()
        val before = timestamp.getAndSet(now)
        val duration = (now - before).nano.toUnit(SECONDS)
        logger.info(s"current status ${currentBatch * batchSize} migrated $batchSize in $duration")
      }
    }
  }

  def batch(queries: Seq[RegularStatement]) = {
    QueryBuilder.unloggedBatch(queries: _*)
  }

  def insertQuery(toSelection: Selection, names: Array[String], values: Array[AnyRef]) = {
    QueryBuilder
      .insertInto(toSelection.keyspace, toSelection.table)
      .values(names, values)
  }

  def process(selectionFrom: Selection, toSelection: Selection, keep: RowData => Boolean, batchSize: Int = Defaults.batchSize): Int = {

    val reporter = Reporter(batchSize = batchSize)
    try {
      val read = selectionToReadQuery(selectionFrom)
      logger.info(s"executing $read")
      CassandraHelper(from)
        .streamQuery(read)
        .map(rowToMap.rowToDoubleArray)
        .map(data => insertQuery(toSelection, data.names, data.data))
        .sliding(batchSize, batchSize)
        .map(batch)
        .foreach { query =>
          to.executeAsync {
            query
          }.asScala.onComplete {
            case Failure(exception) => logger.warn("insertion failed", exception)
            case Success(value) => reporter.poke()
          }(ExecutionContext.global)

        }
    } catch {
      case NonFatal(e) => logger.error("i failed master", e)
    }
    reporter.count.get()
  }
}

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

//some helper class for people who whant to use filters this is high performance stuff, why just for fun!
case class RowData(names: Array[String], data: Array[AnyRef]) extends Iterable[(String, AnyRef)] {
  override def iterator: Iterator[(String, AnyRef)] = names.iterator.zip(data.iterator)

  def get(name: String): Option[AnyRef] = {
    var idx = 0
    while (idx < names.length) {
      if (names(idx) == name) {
        return Some(data(idx))
      }
      idx += 1
    }
    None
  }

  def get(idx: Int): Option[(String, AnyRef)] = {
    if (idx < names.length) {
      Some(names(idx) -> data(idx))
    } else {
      None
    }
  }

  /**
    * super fast get only use this when you use an element of names
    */
  def getFastUnsafe(name: String): Option[AnyRef] = {
    var idx = 0
    while (idx < names.length) {
      if (names(idx) eq name) {
        //break
        return Some(data(idx))
      }
      idx += 1
    }
    None
  }
}

object rowToMap {

  def rowToDoubleArray(row: Row) = {
    val definitions = row.getColumnDefinitions.asScala
    val names = new Array[String](definitions.size)
    val data = new Array[AnyRef](definitions.size)
    definitions.zipWithIndex.foreach {
      case (definition, index) =>
        val column = row.getObject(definition.getName)
        names(index) = definition.getName
        data(index) = column
    }
    RowData(names, data)
  }

  def rowToTupleArray(row: Row) = {
    val definitions = row.getColumnDefinitions.asScala
    val output = new Array[(String, AnyRef)](definitions.size)
    definitions.zipWithIndex.foreach {
      case (definition, index) =>
        val column = row.getObject(definition.getName)
        output(index) = (definition.getName -> column)
    }
    output
  }

  def rowToTupleSeq(row: Row) = {
    val definitions = row.getColumnDefinitions.asScala
    val output = Map.newBuilder[String, AnyRef]
    definitions.zipWithIndex.foreach {
      case (definition, index) =>
        val column = row.getObject(definition.getName)
        output.addOne(definition.getName -> column)
    }
    output.result()
  }
}
