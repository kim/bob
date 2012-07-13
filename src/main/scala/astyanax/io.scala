package astyanax

trait IO {

    import java.util.concurrent.{ Callable
                                , Executors
                                , ExecutorService
                                , Future
                                , TimeUnit
                                }

    import scala.collection.JavaConversions._

    import org.apache.cassandra.thrift.{ Cassandra => Thrift }
    import org.apache.thrift.async.TAsyncClientManager
    import org.apache.thrift.protocol.{ TBinaryProtocol
                                      , TProtocol
                                      , TProtocolFactory
                                      }
    import org.apache.thrift.transport.{ TNonblockingSocket, TTransport }

    import Astyanax._
    import ResourcePool._


    case class CassandraConfig[C]
        ( hosts:           Seq[(String, Int)]
        , maxConns:        Int                = 50
        , connIdleTime:    (Long, TimeUnit)   = 500L -> TimeUnit.MILLISECONDS
        , selectorThreads: Int                = 1
        )

    case class CassandraState[C]
        ( pool: Pool[Client[C]]
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
    : Cassandra[C] =
        Cassandra(mk(conf))

    def releaseCassandra[C](c: Cassandra[C]) = c.finalize()

    implicit
    def lift[C, A](t: Task[Client[C], A]): MonadCassandra[C, Future[Result[A]]] =
        state(s => s -> s.exec.submit(callable(
            try   { withResource(s.pool) { c => t(c).eval(c).map(_._2) }}
            catch { case e => Result[A](Left(e)) }
        )))

    implicit
    def mkCassandraState(conf: CassandraConfig[Thrift.AsyncClient])
    : CassandraState[Thrift.AsyncClient] = {

        @volatile var j = 0
        def nextMgr = { j = (j + 1) % conf.selectorThreads; j }
        val mgrs = (0 until conf.selectorThreads).map(_ => new TAsyncClientManager)

        @volatile var i = 0
        def nextClient = { i = (i + 1) % conf.hosts.size; i }
        val newClient = (_: Unit) => {
            val (h, p) = conf.hosts(nextClient)
            ThriftClient(h, p, mgrs(nextMgr))
        }

        val pool = createPool[Client[Thrift.AsyncClient]](
          newClient
        , _.close()
        , conf.hosts.size
        , conf.connIdleTime
        , conf.maxConns
        )

        CassandraState(pool, Executors.newCachedThreadPool)
    }

    private[this]
    def releaseCassandraState[C](s: CassandraState[C]) {
        s.exec.shutdown()
        destroyAll(s.pool)
    }

    private[this]
    def callable[A](f: => A): Callable[A] = new Callable[A] { def call = f }
}

object IO extends IO


// vim: set ts=4 sw=4 et:
