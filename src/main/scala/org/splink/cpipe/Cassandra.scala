package org.splink.cpipe

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.typesafe.scalalogging.LazyLogging

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
        new SocketOptions().setKeepAlive(true)
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

  import org.splink.cpipe.util.FuturesExtended._
  case class CassandraHelper(session: Session) extends LazyLogging{
    def getTables(keyspace: String) = {
      session.getCluster.getMetadata.getKeyspace(keyspace).getTables.asScala.toArray
    }

    private def preFetch(maxCache: Int, rs: ResultSet): Future[ResultSet] = {
      if (rs.getAvailableWithoutFetching < maxCache  && !rs.isFullyFetched) {
        rs.fetchMoreResults().asScala.flatMap { newRs =>
          preFetch(maxCache, newRs)
        }(ExecutionContext.global)
      } else {
        Future.successful(rs)
      }
    }

    def streamQuery(statement: Statement): Iterator[Row] = {

      val rs = session.execute(statement)

      rs.iterator().asScala.map { row =>
        //we are IO bound currently and prefetching can hit 0.
        if(rs.getAvailableWithoutFetching == statement.getFetchSize && !rs.isFullyFetched){
          rs.fetchMoreResults()
        }
        row
      }
    }
  }
}
