package bob.pool

trait HostConnectionPool[C] {
    def withConnection[X](act: C => X): X
    def destroy()
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

        def destroy() = pools.values.map(destroyAll)
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

        def destroy() = pools.values.map(destroyAll)
    }
}
