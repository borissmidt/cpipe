package org.splink.cpipe

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.ArraySeq
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

object Cassandra {

  def apply(
      hosts: Seq[String],
      port: Int = Defaults.cassandraPort,
      username: String = "",
      password: String = "",
      consistencyLevel: ConsistencyLevel = ConsistencyLevel.ONE,
      fetchSize: Int = Defaults.fetchSize,
      timeoutMillis: Int = Defaults.cassandraTimeoutMilis,
      useCompression: Boolean = true,
      clusterName: Option[String] = None
    ): Session = {
    val mb = 1 << 20
    val clusterBuilder = new Cluster.Builder()
      .addContactPoints(hosts: _*)
      .withCompression(
        if (useCompression) ProtocolOptions.Compression.LZ4
        else ProtocolOptions.Compression.NONE
      )
      .withPort(port)
      .withCredentials(username, password)
      .withSocketOptions(
        new SocketOptions()
          .setKeepAlive(true)
          .setConnectTimeoutMillis(timeoutMillis * 2)
          .setReadTimeoutMillis(timeoutMillis)
          .setReceiveBufferSize(32 * mb)
          .setSendBufferSize(32 * mb)
      )
      .withQueryOptions(
        new QueryOptions().setConsistencyLevel(consistencyLevel).setFetchSize(fetchSize)
      )

    val dcBuilder = clusterName match {
      case Some(dcName) =>
        clusterBuilder
          .withLoadBalancingPolicy(
            new TokenAwarePolicy(
              DCAwareRoundRobinPolicy
                .builder()
                .withLocalDc(dcName)
                .withUsedHostsPerRemoteDc(0)
                .build()
            )
          )
      case _ => clusterBuilder
    }

    dcBuilder.build.connect
  }

  /**
    * class is not thread safe, i.e. its an iterator
    * @param rs
    */
  final case class Page(pageMarker: Option[PagingState], rows: Seq[Row])

  trait Cancelable {
    def cancel(): Unit
  }

  //a heapload of stuff to get the next page out of the result :(
  class PagedResultSet(rs: ResultSet) extends Iterator[Page] with Cancelable {
    var iter = rs.iterator().asScala
    var canceled = false
    override def hasNext: Boolean = !rs.isExhausted && !canceled

    def next(): Page = {
      val pageSize = rs.getAvailableWithoutFetching()
      val (fetchedIter, stillInDb) = iter.splitAt(pageSize)
      val fetched = ArraySeq.from(fetchedIter)
      iter = stillInDb
      Page(
        rs.getAllExecutionInfo.asScala.init.lastOption
          .flatMap(x => Option(x.getPagingState)),
        fetched
      )
    }

    override def cancel(): Unit = canceled = true
  }

  import org.splink.cpipe.util.FuturesExtended._
  case class CassandraHelper(session: Session) extends LazyLogging {
    def getTables(keyspace: String) = {
      session.getCluster.getMetadata.getKeyspace(keyspace).getTables.asScala.toArray
    }

    def pagedStream(statement: Statement, offset: Option[PagingState] = None): PagedResultSet = {
      val query = offset.map(statement.setPagingState).getOrElse(statement)
      val rs = session.execute(query)
      new PagedResultSet(rs)
    }
  }
}
