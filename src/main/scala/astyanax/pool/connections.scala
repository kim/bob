package astyanax.pool

trait HostConnectionPool[C] {
    def withConnection[X](act: C => X): X
    def destroy()
}

object HostConnectionPool extends HostConnectionPools

trait HostConnectionPools {

    import scala.util.Random
    import ResourcePool._

    def RandomPool[C](pools: Map[(String, Int), Pool[C]])
    : HostConnectionPool[C] = new HostConnectionPool[C] {
        private[this] val ps = pools.values.toIndexedSeq

        def withConnection[X](act: C => X): X =
            withResource(ps(Random.nextInt(ps.size)))(act)

        def destroy() = pools.values.map(destroyAll)
    }
}
