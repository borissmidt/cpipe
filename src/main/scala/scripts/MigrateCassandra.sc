import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.splink.cpipe.Cassandra.CassandraHelper
import org.splink.cpipe.Defaults._
import org.splink.cpipe.Dsl._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

val betweenClusters = fromCassandra(Seq("cassandra1")).to(Seq("localhost"))
CassandraHelper(betweenClusters.from).getTables("tso")
val timeformat = java.time.format.DateTimeFormatter.ISO_DATE_TIME
def parseTimestamp(timestamp: String) = {
  java.time.ZonedDateTime.parse(timestamp,timeformat).toInstant
}

val aYearAgo = ZonedDateTime.now(ZoneOffset.UTC).minusYears(1).toInstant

val migration1 =betweenClusters
  .migrate("tso", "twc_forecast_weather_latest")
  .to("tso", "twc_forecast_weather_latest").withFilter
{ rowData =>
  rowData.contains("timestamp"){
    case timestamp: Date if timestamp.toInstant.isAfter(aYearAgo) => true
    case _ => false
  }
}
  .runAsync

val migration2 = betweenClusters
  .migrate("keyspace", "table")
  .to("keyspace", "table2")
  .withFilter{ rowData =>
    rowData.contains("timestamp") {
      case timestamp: Date if timestamp.toInstant.isAfter(aYearAgo) => true
      case _ => false
    }
  }
  .runAsync

Await.ready(
  Future.sequence(Seq(migration1,migration2)),
  100.days
)