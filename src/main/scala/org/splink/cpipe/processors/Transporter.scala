package org.splink.cpipe.processors
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.datastax.driver.core.querybuilder.{Batch, QueryBuilder}
import com.datastax.driver.core.{BatchStatement, RegularStatement, Session, SimpleStatement}
import com.typesafe.scalalogging.LazyLogging
import org.splink.cpipe.Cassandra.CassandraHelper
import org.splink.cpipe.Defaults
import org.splink.cpipe.config.Selection
import org.splink.cpipe.util.FuturesExtended._
import org.splink.cpipe.util.{RowConversions, RowData}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal
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

  def makeBatch(queries: Seq[RegularStatement]) = {
    QueryBuilder.unloggedBatch(queries: _*)
  }

  def insertQuery(toSelection: Selection, names: Array[String], values: Array[AnyRef]) = {
    QueryBuilder
      .insertInto(toSelection.keyspace, toSelection.table)
      .values(names, values)
  }

  private def executeBatch(query: Batch) = {
    Future {
      to.execute {
        query
      }
    }(Defaults.ioPool)
      .onComplete {
        case Success(value) => value
        case Failure(exception) => logger.warn(s"insertion failed ${query}", exception)
      }(ExecutionContext.global)
    }

  def process(selectionFrom: Selection, toSelection: Selection, keep: RowData => Option[RowData], batchSize: Int = Defaults.batchSize): Int = {
    val reporter = Reporter(batchSize = batchSize)
    try {
      val total = new AtomicLong()
      val inserted = new AtomicLong()

      val read = selectionToReadQuery(selectionFrom)
      logger.info(s"executing $read")
      CassandraHelper(from)
        .streamQuery(read)
        .map(RowConversions.rowToTwinArray)
        .tapEach{_ =>
          val count = total.incrementAndGet()
          val insertedCount = inserted.get()
          if(count % 10000 == 0) {
            logger.info(s"for migration ${selectionFrom.table} -> ${toSelection.table} inserted $insertedCount of $count")
          }
        }
        .flatMap(keep) //filter step
        .tapEach{_ =>inserted.incrementAndGet()}
        .map(data => insertQuery(toSelection, data.names, data.data))
        .sliding(batchSize, batchSize)
        .map(makeBatch)
        .foreach { executeBatch }
    } catch {
      case NonFatal(e) => logger.error("i failed master", e)
    }
    reporter.count.get()
  }
}

