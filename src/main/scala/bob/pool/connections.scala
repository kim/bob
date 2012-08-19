package bob.pool

trait HostConnectionPool[C] {
    def withConnection[X](act: C => X): X
    def destroy()
    def numActive: Int
    def numAllocated: Int
    def remainingCapacity: Int
}

object HostConnectionPool extends HostConnectionPools

trait HostConnectionPools {

    import scala.util.Random
    import ResourcePool._

    type Host = (String, Int)

    // randomly select the host connection. may block.
    def RandomPool[C](pools: Map[Host, Pool[C]])
    : HostConnectionPool[C] = new HostConnectionPool[C] {
        private[this] val ps = pools.values.toIndexedSeq

        def withConnection[X](act: C => X): X =
            withResource(ps(Random.nextInt(ps.size)))(act)

        def destroy()         = _destroy(pools)
        def numAllocated      = _numAllocated(pools)
        def numActive         = _numActive(pools)
        def remainingCapacity = _remainingCapacity(pools)
    }

    // round-robin over the hosts. may block.
    def RoundRobinPool[C](pools: Map[Host, Pool[C]])
    : HostConnectionPool[C] = new HostConnectionPool[C] {
        @volatile private[this] var i = 0
        private[this] val ps = pools.values.toIndexedSeq

        def withConnection[X](act: C => X): X = {
            i = (i + 1) % ps.size
            withResource(ps(i))(act)
        }

        def destroy()         = _destroy(pools)
        def numAllocated      = _numAllocated(pools)
        def numActive         = _numActive(pools)
        def remainingCapacity = _remainingCapacity(pools)
    }

    private[this]
    def _destroy[C](pools: Map[Host, Pool[C]]) =
        pools.values.foreach(destroyAll)

    private[this]
    def _numAllocated[C](pools: Map[Host, Pool[C]]): Int =
        pools.values.map(numAllocated).sum

    private[this]
    def _numActive[C](pools: Map[Host, Pool[C]]): Int =
        pools.values.map(numActive).sum

    private[this]
    def _remainingCapacity[C](pools: Map[Host, Pool[C]]): Int =
        pools.values.map(remainingCapacity).sum
}
