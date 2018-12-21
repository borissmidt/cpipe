package example.processors

import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}
import example.{Config, Output, Rps}
import play.api.libs.json.{JsObject, Json}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration._
import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

class Exporter2 extends Processor {

  import example.JsonColumnParser._
  val printEc = ExecutionContext.fromExecutor(new ForkJoinPool(8))
  val rps = new Rps()

  class Stats {
    var misses = 0
    var hits = 0

    def miss = synchronized(misses = misses + 1)
    def hit = synchronized(hits = hits + 1)

    def hitPercentage(total: Int) = Math.round(misses / total.toDouble * 10000) / 100d
  }

  val stats = new Stats()


  override def process(session: Session, config: Config): Int = {
    val showProgress = config.flags.showProgress

    val meta = session.getCluster.getMetadata
    val keys = meta.getKeyspace(config.selection.keyspace)
      .getTable(config.selection.table).getPartitionKey.asScala.map(key => key.getName)

    val tokenId = s"token(${keys.mkString(",")})"

    if(config.flags.verbose) {
      Output.log(s"data is spread across ${meta.getAllHosts.size} hosts.")
      Output.log(s"Partitioner is '${meta.getPartitioner}'")
    }

    val rangesByHost = meta.getTokenRanges.asScala.toList.map { range =>
      Set(meta.getReplicas(config.selection.keyspace, range).asScala.head) -> range
    }

    val compactedRanges = Compact(rangesByHost, config.flags.verbose)
      .foldLeft(List.empty[TokenRange]) { case (acc, (_, ranges)) =>
      ranges ::: acc
    }.sorted

    if(config.flags.verbose) Output.log(s"Got ${compactedRanges.size} compacted ranges")

    val groupedRanges = compactedRanges.grouped(config.settings.threads).toList

    if (showProgress && config.flags.verbose)
      Output.update(s"Query ${compactedRanges.size} ranges, ${config.settings.threads} in parallel.")

    def fetchGroups(groups: List[List[TokenRange]]): Future[Unit] = {
      groups match {
        case Nil =>
          Future.successful(())
        case head :: tail =>
          fetchNextGroup(head).map { _ =>
            fetchGroups(tail)
          }.recover {
            case NonFatal(e) =>
              Output.log(s"Error during 'import': message: '${if (e != null) e.getMessage else ""}'")
              //TODO add counter to give up after a couple of retries
              fetchGroups(groups)
          }.flatten
      }
    }

    def fetchNextGroup(group: List[TokenRange]) = {
      Future.traverse(group) { range =>
        fetchRows(range).map {
          case results if results.nonEmpty =>
            stats.hit
            outputProgress()
            output(results)
          case _ =>
            stats.miss
            outputProgress()
            Future.successful(())
        }.recover {
          case NonFatal(e) =>
            Output.log(s"Ooops, could not fetch a row. message: ${if (e != null) e.getMessage else ""}")
            Future.successful(())
        }
      }
    }

    def fetchRows(range: TokenRange) = {
      val statement = new SimpleStatement(
        s"select * from ${config.selection.table} where $tokenId > ${range.getStart} and $tokenId <= ${range.getEnd};")

      session.executeAsync(statement).map { rs =>
        rs.iterator().asScala.map { row =>
          if (rs.getAvailableWithoutFetching < statement.getFetchSize / 2 && !rs.isFullyFetched) {
            if (showProgress) Output.update(s"Got ${rps.count} rows, off to get more...")
            rs.fetchMoreResults()
          }

          row2Json(row)
        }
      }
    }

    def outputProgress() = {
      if (showProgress) {
        Output.update(s"${rps.count} rows at $rps rows/sec. " +
          (if(config.flags.verbose)
            s"${stats.hitPercentage(compactedRanges.size)}% misses " +
              s"(${stats.misses} of ${compactedRanges.size} ranges. ${stats.hits} hits)" else "")
        )
      }
    }

    def output(results: Iterator[JsObject]) = {
      results.foreach { result =>
        rps.compute()
        Output.render(Json.prettyPrint(result))
      }
    }

    Await.result(fetchGroups(groupedRanges), Inf)
    rps.count
  }

  implicit def asFuture(resultSet: ResultSetFuture): Future[ResultSet] = {
    val promise = Promise[ResultSet]
    Futures.addCallback[ResultSet](resultSet, new FutureCallback[ResultSet] {
      override def onSuccess(result: ResultSet): Unit = promise.success(result)

      override def onFailure(t: Throwable): Unit = promise.failure(t)
    })
    promise.future
  }
}
