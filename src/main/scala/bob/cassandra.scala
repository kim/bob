package bob


trait Cassandra {

    import java.util.concurrent.{ Callable
                                , Executors
                                , ExecutorService
                                , Future
                                , TimeUnit
                                }

    import Bob._
    import Util._
    import pool.HostConnectionPool


    type MkExecutor = Unit => ExecutorService

    case class CassandraConfig[C]
        ( hosts:           Seq[(String, Int)]
        , mkExecutor:      MkExecutor
        , maxConns:        Int              = 50
        , connIdleTime:    (Long, TimeUnit) = 500L -> TimeUnit.MILLISECONDS
        , selectorThreads: Int              = 1
        )

    case class CassandraState[C]
        ( pool: HostConnectionPool[Client[C]]
        , exec: ExecutorService
        )
    type MkCassandraState[C] = CassandraConfig[C] => CassandraState[C]

    type MonadCassandra[C, A] = State[CassandraState[C], A]

    final case class Cassandra[C](private val s: CassandraState[C]) {
        final def apply[A](m: MonadCassandra[C, A]): A = runWith(s)(m)

        override def finalize() = releaseCassandraState(s)
    }


    def runCassandra[C, A]
        (conf: CassandraConfig[C])(m: MonadCassandra[C, A])
        (implicit mk: MkCassandraState[C])
    : A = {
        val s = mk(conf)
        try     { runWith(s)(m) }
        finally { releaseCassandraState(s) }
    }

    def runWith[C, A](s: CassandraState[C])(m: MonadCassandra[C, A]): A =
        m.apply(s)._2

    def newCassandra[C](conf: CassandraConfig[C])(implicit mk: MkCassandraState[C])
    : Cassandra[C] = Cassandra(mk(conf))

    def releaseCassandra[C](c: Cassandra[C]) = c.finalize()

    implicit
    def lift[C, A](t: Task[Client[C], A]): MonadCassandra[C, Future[Result[A]]] =
        state(s => s -> s.exec.submit(callable {
            val t1 = now()
            try   { s.pool.withConnection { c => t(c).eval(c).map(_._2) }}
            catch { case e => Result[A](Left(e), Latency(t1)) }
        }))

    private[this]
    def releaseCassandraState[C](s: CassandraState[C]) {
        s.exec.shutdown()
        s.pool.destroy()
    }

    private[this]
    def callable[A](f: => A): Callable[A] = new Callable[A] { def call = f }
}


// vim: set ts=4 sw=4 et: