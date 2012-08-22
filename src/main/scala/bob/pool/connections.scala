package bob.pool

import ResourcePool._
import bob.Bob.Result
import bob.Util._

abstract class HostConnectionPool[C](pools: Map[(String, Int), Pool[C]]) {
    def withConnection[X](act: C => Result[X]): Result[X]

    def destroy() =
        pools.values.foreach(ResourcePool.destroyAll)

    def numAllocated: Int =
        pools.values.map(ResourcePool.numAllocated).sum

    def numActive: Int =
        pools.values.map(ResourcePool.numActive).sum

    def remainingCapacity: Int =
        pools.values.map(ResourcePool.remainingCapacity).sum
}

object HostConnectionPool extends HostConnectionPools

trait HostConnectionPools {

    type Host = (String, Int)

    // randomly select the host connection. may block.
    def RandomPool[C](pools: Map[Host, Pool[C]]): HostConnectionPool[C]
    = new HostConnectionPool[C](pools) {
        private[this] val ps = pools.values.toIndexedSeq

        def withConnection[X](act: C => Result[X]) =
            withResource(ps(util.Random.nextInt(ps.size)))(act)
    }

    // round-robin over the hosts. may block.
    def RoundRobinPool[C](pools: Map[Host, Pool[C]]): HostConnectionPool[C]
    = new HostConnectionPool[C](pools) {
        private[this] val i = Stream.continually(pools.values).flatten.iterator

        def withConnection[X](act: C => Result[X]) = withResource(i.next)(act)
    }

    // prefers hosts with low latencies, but skips to attempt resource
    // acquisition if a pool is exhausted. may block only if all pools are
    // exhausted.
    //
    // note that the latency may be that of a series of sequential operations.
    // thus, the score may be skewed if actions are sporadically larger than
    // others. also note that error results are sampled with a factor of 2.
    def LatencyAwarePool[C](pools: Map[Host, Pool[C]]): HostConnectionPool[C]
    = new HostConnectionPool[C](pools) {

        import java.util.concurrent.atomic.AtomicReference

        private[this]
        val scores = Map() ++ pools.keys.map(_ -> new LatencyScore())

        private[this]
        val sortedHosts = new AtomicReference[Iterable[Host]](pools.keys)

        def withConnection[X](act: C => Result[X]) =
            sortedHosts.get
                .dropWhile(h => pools.get(h).map(exhausted).getOrElse(true))
                .headOption
                .orElse(sortedHosts.get.headOption)
                .map(h =>
                    withResource(pools(h))(act) match {
                        case r@Result(Left(_), latency) =>
                            sample(h, latency.copy(l = latency.l * 2))
                            r
                        case r@Result(Right(_), latency) =>
                            sample(h, latency)
                            r
                    }
                )
                .getOrElse(Result(Left(new Exception("No host")), Latency()))

        private[this]
        def sample(h: Host, l: Latency) =
            if (!scores(h).sample(l))
                sortedHosts.lazySet(scores.toList.sortBy(_._2).map(_._1))
    }

    private[this] final
    class LatencyScore(windowSize: Int = 100) {

        import scala.collection.JavaConverters._
        import java.util.concurrent.ArrayBlockingQueue

        private[this]
        val latencies = new ArrayBlockingQueue[Latency](windowSize)

        @volatile private[this]
        var _score = 0.0d

        def sample(s: Latency): Boolean =
            !(latencies.offer(s) || {
                _score = mean
                try { latencies.remove() } catch { case e => }
                latencies.offer(s)
                false
            })

        def score: Double = _score

        def mean: Double = {
            val (cnt,sum) = latencies.asScala.foldLeft(0L -> 0L) {
                case ((cnt,sum), lat) => (cnt + 1) -> (sum + lat.l)
            }

            if (cnt > 0) sum / cnt else 0.0d
        }
    }

    implicit private[this]
    val LatencyScoreOrdering: Ordering[LatencyScore] = new Ordering[LatencyScore] {
        def compare(a: LatencyScore, b: LatencyScore) = a.score compare b.score
    }
}
