package org.splink.cpipe.config

import com.datastax.driver.core.ConsistencyLevel

final case class Config(mode: String, from: Connection, to: Option[Connection], selection: Selection, toSelection: Selection,flags: Flags, settings: Settings)

case object Config {
  def fromArguments(args: Arguments) ={
    val helper = ArgsHelper(args)
    import args._
    for {
      fromConnection <- helper.fromConnection
      toConnection = helper.toConnection
      fromSelection <- helper.fromSelection
      toSelection <- helper.toSelection
      settings <- helper.settings
      flags <- helper.flags
      mode <- mode.toOption

    } yield {
      Config(mode,
        fromConnection,
        toConnection,
        fromSelection,
        toSelection,
        flags,
        settings,
      )
    }
  }

  case class ArgsHelper(args: Arguments) {
    //prefixing everything with args doesn't help readability
    import args._

    def fromConnection = {
      for {
        hosts <- hosts.toOption
        port <- port.toOption
        username <- username.toOption
        password <- password.toOption
      } yield {
        Connection(hosts.split(',').toSeq, port, Credentials(username, password))
      }
    }

    def toConnection = {
      for {
        hosts <- toHosts.toOption
        port <- toPort.toOption
        username <- toUsername.toOption
        password <- toPassword.toOption
      } yield {
        Connection(hosts.split(',').toSeq, port, Credentials(username, password))
      }
    }

    def fromSelection = {
      for {
        filter <- filter.toOption.map(_.mkString(" "))
        table <- table.toOption
        keyspace <- keyspace.toOption
      } yield {
        Selection(keyspace, table, filter)
      }
    }

    def toSelection = {
      for {
        filter <- filter.toOption.map(_.mkString(" "))
        table <- toTable.toOption.orElse(table.toOption)
        keyspace <- keyspace.toOption.orElse(keyspace.toOption)
      } yield {
        Selection(keyspace, table, filter)
      }
    }

    def settings = {
      import ConsistencyLevel._
      for{
        fetchSize <- fetchSize.toOption
        batchSize <- batchSize.toOption
        threads <- threads.toOption
        consistencyLevel <- consistencyLevel.toOption.map {
          case cl if cl == ANY.name() => ANY
          case cl if cl == ONE.name() => ONE
          case cl if cl == TWO.name() => TWO
          case cl if cl == THREE.name() => THREE
          case cl if cl == QUORUM.name() => QUORUM
          case cl if cl == ALL.name() => ALL
          case cl if cl == LOCAL_QUORUM.name() => LOCAL_QUORUM
          case cl if cl == EACH_QUORUM.name() => EACH_QUORUM
          case cl if cl == SERIAL.name() => SERIAL
          case cl if cl == LOCAL_SERIAL.name() => LOCAL_SERIAL
          case cl if cl == LOCAL_ONE.name() => LOCAL_ONE
        }
      } yield {
        Settings(fetchSize, batchSize, consistencyLevel, threads)
      }
    }

    def flags = {
      for{
        beQuiet <- quiet.toOption
        verbose <- verbose.toOption
        useCompression <- compression.toOption.map {
          case c if c == "ON" => true
          case _ => false
        }
      } yield {
        Flags(!beQuiet, useCompression, verbose)
      }
    }

  }




}

final case class Connection(hosts: Seq[String], port: Int, credentials: Credentials)

final case class Selection(keyspace: String, table: String, filter: String = "")

final case class Credentials(username: String, password: String)

final case class Flags(showProgress: Boolean, useCompression: Boolean, verbose: Boolean)

final case class Settings(fetchSize: Int, batchSize: Int, consistencyLevel: ConsistencyLevel, threads: Int, timeoutMillis: Int = Defaults.cassandraTimeoutMilis)
