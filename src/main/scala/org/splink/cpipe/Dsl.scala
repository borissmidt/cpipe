package org.splink.cpipe

import com.datastax.driver.core.Session
import org.splink.cpipe.Dsl.Internal.Source
import org.splink.cpipe.config.{Selection, Settings}
import org.splink.cpipe.processors.Transporter
import org.splink.cpipe.util.RowData

import scala.concurrent.{ExecutionContext, Future}

/**
 * Copyright (C) 10.02.20 - REstore NV
 */

object Dsl {
  def fromCassandra(host: Seq[String], port: Int = Defaults.cassandraPort, username: String ="", password: String = "")(implicit s: Settings)={
    Source(Internal.sessionFrom(host, port, username, password))
  }

  object Internal{
    def sessionFrom(host: Seq[String], port: Int, username: String, password: String)(implicit s: Settings) = {
      Cassandra(host, port, username, password, s.consistencyLevel,s.fetchSize,s.timeoutMillis,s.useCompression)
    }

    case class Source(source: Session){
      def to(host: Seq[String], port: Int = Defaults.cassandraPort, username: String ="", password: String = "")(implicit s: Settings) ={
        CassandraToCassandra(source, sessionFrom(host,port,username,password))
      }
    }

    case class CassandraToCassandra(from:  Session, to: Session){
        def migrate(keyspace: String, table: String, selection: String = "") = {
          DateSource(this, keyspace, table, selection, Some(_))
        }

    }

    case class DateSource(cassandra: CassandraToCassandra, fromKeyspace: String, fromTable: String, selection: String, modifierAndFilter: RowData => Option[RowData]){

      def withFilter(filter: RowData => Boolean) = {
        this.copy(modifierAndFilter = Some(_).filter(filter))
      }

      /**
       *
       * @param keyspace the origin keyspace
       * @param table the origin table
       * @param selection an optional query part used in the cassandra query
       * @param modifierAndFilter a way to modify the data, Return Some to keep return None to remove.
       * @return
       */
      def withFilterModifer(filter: RowData => Option[RowData]) = {
        this.copy(modifierAndFilter = filter)
      }

      def to(keyspace: String, table: String) ={
        Migration(this, keyspace, table)
      }

    }

    case class Migration(source: DateSource, toKeyspace: String, toTable: String){
      import source._

      def run = {
        Transporter(cassandra.from, cassandra.to).process(Selection(fromKeyspace,fromTable,  selection),Selection(toKeyspace,toTable), modifierAndFilter)
      }

      def runAsync(implicit ex: ExecutionContext = Defaults.ioPool) = Future{
        run
      }

      def withFilter(filter: RowData => Boolean) = {
        this.copy(source=source.copy(modifierAndFilter = Some(_).filter(filter)))
      }

      /**
       *
       * @param keyspace the origin keyspace
       * @param table the origin table
       * @param selection an optional query part used in the cassandra query
       * @param modifierAndFilter a way to modify the data, Return Some to keep return None to remove.
       * @return
       */
      def withFilterModifer(filter: RowData => Option[RowData]) = {
        this.copy(source=source.copy(modifierAndFilter = filter))
      }
    }
  }

}
