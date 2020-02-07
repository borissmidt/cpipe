package org.splink.cpipe

import org.splink.cpipe.CPipe.{ElapsedSecondFormat, createSessionFrom, exportConfig}
import org.splink.cpipe.processors.{Exporter, Exporter2, Importer, Importer2, Transporter}
import org.splink.cpipe.config.{Arguments, Config, Connection}

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class CPipe(args: Array[String]) {
  Config.fromArguments(new Arguments(args.toSeq)).foreach { config =>
    if (config.flags.showProgress) Output.update("Connecting to cassandra.")

    val session = createSessionFrom(config.from, config)
    session.execute(s"use ${config.selection.keyspace}")

    if (config.flags.showProgress)
      Output.update(s"Connected to cassandra ${session.getCluster.getClusterName}")

    val start = System.currentTimeMillis()

    val rowCount = Try {
      config.mode match {
        case "import" =>
          new Importer().process(session, config)
        case "import2" =>
          new Importer2().process(session, config)
        case "export" =>
          new Exporter().process(session, exportConfig(config))
        case "export2" =>
          if (session.getCluster.getMetadata.getPartitioner == "org.apache.cassandra.dht.Murmur3Partitioner") {
            new Exporter2().process(session, exportConfig(config))
          } else {
            Output.log("mode 'export2' requires the cluster to use 'Murmur3Partitioner'")
          }
        case "transporter" => {
          val toSession = createSessionFrom(config.to.get, config)
          Transporter(session, toSession).process(config.selection,config.toSelection, _ => true )
        }
      }
    } match {
      case Success(count) => count
      case Failure(e) =>
        Output.log(
          s"\nError during '${config.mode}': message: '${if (e != null) e.getMessage else ""}'"
        )
        System.exit(1)
        0
    }

    if (config.flags.showProgress) {
      val sec = (System.currentTimeMillis() - start) / 1000
      Output.log(s"\nProcessing $rowCount rows took ${ElapsedSecondFormat(sec)}s")
    }
  }
}

object CPipe {

  def main(args: Array[String]): Unit = {
    new CPipe(args)
    System.exit(0)
  }

  def createSessionFrom(connection: Connection, conf: Config) =
    Cassandra(
      connection.hosts,
      connection.port,
      connection.credentials.username,
      connection.credentials.password,
      conf.settings.consistencyLevel,
      conf.settings.fetchSize,
      conf.settings.timeoutMillis,
      conf.flags.useCompression
    )

  def exportConfig(config: Config): Config = {
    if (config.settings.threads != 1) {
      Output.log("Export is limited to 1 thread")
      config.copy(settings = config.settings.copy(threads = 1))
    } else {
      config
    }
  }

  object ElapsedSecondFormat {
    def zero(i: Long) = if (i < 10) s"0$i" else s"$i"

    def apply(s: Long) =
      s"""${zero(s / 3600)}:${zero((s % 3600) / 60)}:${zero(s % 60)}"""
  }

}
