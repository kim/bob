package bob

package object thrift extends Api with ClientInstances {

    import org.apache.cassandra.thrift.Cassandra
    import org.apache.thrift.async.TAsyncClientManager

    import Bob._
    import pool.HostConnectionPool._
    import pool.ResourcePool._

    type AsyncClient = Cassandra.AsyncClient

    implicit
    def mkCassandraState[F[_]](conf: CassandraConfig[AsyncClient, F])
    : CassandraState[AsyncClient, F] = {
        val mgrs = Stream.continually(
          (0 until conf.selectorThreads).map(_ => new TAsyncClientManager)
        ).flatten
        val pools = Map() ++ conf.hosts.zip(mgrs).map { case (hp@(h,p),mgr) =>
            hp -> createPool[ThriftClient]( _ => ThriftClient(h,p,mgr)
                                          , _ close ()
                                          , 1
                                          , conf.connIdleTime
                                          , conf.maxConns
                                          )
        }

        CassandraState(RandomPool(pools), conf.mkExecutor())
    }
}


// vim: set ts=4 sw=4 et:
