package astyanax

trait IO {

    import java.util.concurrent.{ ArrayBlockingQueue
                                , BlockingQueue
                                , Callable
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

    import Clients._
    import ResourcePool._
    import Typeclasses._


    case class CassandraConfig
        ( hosts:        Seq[(String, Int)]
        , maxConns:     Int                = 50
        , connIdleTime: (Long, TimeUnit)   = 500L -> TimeUnit.MILLISECONDS
        )

    case class CassandraState[C]
        ( pool: Pool[Client[C]]
        , exec: ExecutorService
        )
    type MkCassandraState[C] = CassandraConfig => CassandraState[C]

    type MonadCassandra[A, C] = State[CassandraState[C], A]

    final case class Cassandra[C](private val s: CassandraState[C]) {
        final def apply[A](m: MonadCassandra[A, C]): A = runWith(s)(m)

        override def finalize() = releaseCassandraState(s)
    }


    def runCassandra[A, C]
        (conf: CassandraConfig)(m: MonadCassandra[A, C])
        (implicit mk: MkCassandraState[C])
    : A = {
        val s = mk(conf)
        try     { runWith(s)(m) }
        finally { releaseCassandraState(s) }
    }

    def runWith[A, C](s: CassandraState[C])(m: MonadCassandra[A, C]): A =
        m.apply(s)._2

    def newCassandra[C](conf: CassandraConfig)(implicit mk: MkCassandraState[C])
    : Cassandra[C] =
        Cassandra(mk(conf))

    implicit
    def lift[A, C](t: Task[Client[C], A])
    : MonadCassandra[Future[Result[A]], C] = {

        def go[A](t: Task[Client[C], A])(s: CassandraState[C])
        : Future[Result[A]] =
            s.exec.submit(callable(
              withResource(s.pool) { c => t(c).eval(c).map(_._2) }
            ))

        state(s => s -> {
            try   { go(t)(s) }
            catch { case e => future(Result[A](Left(e))) }
        })
    }

    private[this]
    def future[A](r: => Result[A]): Future[Result[A]] =
        new Future[Result[A]] {
            def get() = r
            def get(timout: Long, unit: TimeUnit) = get
            def cancel(mayInterrupt: Boolean) = false
            def isCancelled() = false
            def isDone() = true
        }

    implicit
    def mkCassandraState(conf: CassandraConfig)
    : CassandraState[Thrift.AsyncClient] = {
        val mgr = new TAsyncClientManager
        val addrs = Stream.continually(conf.hosts).flatten.iterator
        val newClient = (_:Unit) => {
            val (h, p) = addrs.next
            ThriftClient(h, p, mgr)
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
    def releaseCassandraState(s: CassandraState[_]) {
        s.exec.shutdown()
        destroyAll(s.pool)
    }

    private[this]
    def callable[A](f: A): Callable[A] = new Callable[A] { def call = f }
}

object IO extends IO


// vim: set ts=4 sw=4 et:
