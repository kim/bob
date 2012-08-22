package bob


trait Cassandra {

    import Bob._
    import Util._
    import pool.HostConnectionPool
    import pool.ResourcePool.Pool


    type MkThreadPool[F[_]] = Unit => ThreadPool[F]

    type MkConnPool[C] = Map[Host, Pool[C]] => HostConnectionPool[C]

    type MkCassandraState[C, F[_]] = CassandraConfig[C,F] => CassandraState[C,F]

    type MonadCassandra[C, A, F[_]] = State[CassandraState[C,F], A]

    case class CassandraConfig[C, F[_]]
        ( hosts:           Seq[Host]
        , mkConnPool:      MkConnPool[Client[C]]
        , mkThreadPool:    MkThreadPool[F]
        , maxConnsPerHost: Int      = 50
        , connIdleTime:    Duration = 500.milliseconds
        , selectorThreads: Int      = 1
        )

    case class CassandraState[C, F[_]]
        ( pool: HostConnectionPool[Client[C]]
        , exec: ThreadPool[F]
        )

    final case class Cassandra[C, F[_]](private val s: CassandraState[C, F]) {
        final def apply[A](m: MonadCassandra[C, A, F]): A = runWith(s)(m)

        override def finalize() = releaseCassandraState(s)
    }


    def runCassandra[C, A, F[_]]
        ( conf: CassandraConfig[C, F]
        )
        ( m: MonadCassandra[C, A, F]
        )
        ( implicit mk: MkCassandraState[C, F]
        )
    : A = {
        val s = mk(conf)
        try     { runWith(s)(m) }
        finally { releaseCassandraState(s) }
    }

    def runWith[C, A, F[_]]
        ( s: CassandraState[C, F]
        )
        ( m: MonadCassandra[C, A, F]
        )
    : A =
        m.apply(s)._2

    def newCassandra[C, F[_]]
        ( conf: CassandraConfig[C, F]
        )
        ( implicit mk: MkCassandraState[C,F]
        )
    : Cassandra[C, F] = Cassandra(mk(conf))

    def releaseCassandra[C, F[_]](c: Cassandra[C, F]) = c.finalize()

    implicit
    def lift[C, A, F[_]](t: Task[Client[C], A])
    : MonadCassandra[C, F[Result[A]], F] =
        state(s => s -> s.exec.submit(callable {
            val t1 = now()
            try   { s.pool.withConnection { c => t(c).eval(c).map(_._2) }}
            catch { case e => Result[A](Left(e), Latency(t1)) }
        }))

    private[this]
    def releaseCassandraState[C, F[_]](s: CassandraState[C, F]) {
        s.exec.shutdown()
        s.pool.destroy()
    }
}


// vim: set ts=4 sw=4 et:
