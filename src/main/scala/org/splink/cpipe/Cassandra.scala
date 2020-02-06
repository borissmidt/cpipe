package org.splink.cpipe

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.typesafe.scalalogging.LazyLogging
import org.splink.cpipe.config.Defaults

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
      dc: Option[String] = None
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

    val dcBuilder = dc match {
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

  import org.splink.cpipe.processors.FuturesExtended._
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
        logger.info("finished prefetch")
        Future.successful(rs)
      }
    }

    def streamQuery(statement: Statement): Iterator[Row] = {

      val rs = session.execute(statement)
      val maxCache = statement.getFetchSize * 16

      rs.iterator().asScala.map { row =>
        //we are IO bound currently and prefetching can hit 0.
        if(rs.getAvailableWithoutFetching == 0 && !rs.isFullyFetched){
          logger.info("starting prefetch")
          preFetch(maxCache, rs)
        }
        row
      }
    }
  }
}
