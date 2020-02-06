package org.splink.cpipe.processors

import com.datastax.driver.core.{Session, SimpleStatement}
import org.splink.cpipe.Cassandra.CassandraHelper
import org.splink.cpipe.{Output, Rps}
import org.splink.cpipe.JsonColumnParser._
import org.splink.cpipe.config.Config
import play.api.libs.json.Json

import scala.jdk.CollectionConverters._

class Exporter extends Processor {

  val rps = new Rps()

  override def process(session: Session, config: Config): Int = {
    val showProgress = config.flags.showProgress

    if (config.flags.showProgress) Output.update("Execute query.")

    val statement = new SimpleStatement(s"select * from ${config.selection.table} ${config.selection.filter};")

    CassandraHelper(session)
      .streamQuery(statement)
      .foreach { row =>
      rps.compute()
      if (showProgress) Output.update(s"${rps.count} rows at $rps rows/sec.")

      val json = row2Json(row)
      Console.println(Json.prettyPrint(json))
    }
    rps.count
  }

}
