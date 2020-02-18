package scripts

import java.time.Instant

/**
  * Copyright (C) 18.02.20 - REstore NV
  */
object Main extends App {
  import java.time.{ZoneOffset, ZonedDateTime}

  import org.splink.cpipe.Cassandra.CassandraHelper
  import org.splink.cpipe.Defaults._
  import org.splink.cpipe.Dsl._

  val betweenClusters = fromCassandra(Seq("cassandra1")).to(Seq("localhost"))
  CassandraHelper(betweenClusters.from).getTables("tso")
  val timeformat = java.time.format.DateTimeFormatter.ISO_DATE_TIME
  def parseTimestamp(timestamp: String) = {
    java.time.ZonedDateTime.parse(timestamp, timeformat).toInstant
  }

  val aYearAgo = ZonedDateTime.now(ZoneOffset.UTC).minusYears(1).toInstant

//  val migration1 = betweenClusters
//    .migrate("tso", "twc_forecast_weather_latest")
//    .to("tso", "twc_forecast_weather_latest")
//    .withFilter { rowData =>
//      rowData.contains("timestamp") {
//        case timestamp: Date if timestamp.toInstant.isAfter(aYearAgo) => true
//        case _ => false
//      }
//    }
//    .run

  def quickMigration(
      keyspaceFrom: String,
      tableFrom: String,
      tableTo: String,
      keyspaceTo: String
    ) = {
    betweenClusters
      .migrate(keyspaceFrom, tableFrom)
      .to(keyspaceTo, tableTo)
      .withFilter { rowData =>
        rowData.contains("timestamp") {
          case timestamp: Instant if timestamp.isAfter(aYearAgo) => {
            println(timestamp)
            true
          }
          case timestamp: Instant => false
        }
      }
  }

  quickMigration(
    "tso",
    "table",
    "tso",
    "table2"
  ).runAsync
  while (true) {
    Thread.sleep(1000)
  }
//  Await.ready(
//    Future.sequence(Seq(migration1, migration2)),
//    100.days
//  )
}
