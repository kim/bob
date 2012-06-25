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

    import org.apache.cassandra.thrift._
    import org.apache.thrift.async._
    import org.apache.thrift.protocol._
    import org.apache.thrift.transport._

    import ResourcePool._
    import Types._


    case class CassandraConfig
        ( hosts:        Seq[(String, Int)]
        , maxConns:     Int                = 50
        , connIdleTime: (Long, TimeUnit)   = 500L -> TimeUnit.MILLISECONDS
        )

    case class CassandraState
        ( pool: Pool[Client]
        , exec: ExecutorService
        )

    type Cassandra[A] = State[CassandraState, A]

    def runCassandra[A](conf: CassandraConfig)(m: Cassandra[A]): A =
        m.apply(mkCassandraState(conf))._2

    implicit
    def lift[A](t: Task[A]): Cassandra[Result[A]] = {
        def go[A](t: Task[A])(s: CassandraState): Future[Result[A]] =
            s.exec.submit(withResource(s.pool) { c =>
                val ret = t(c).get._1
                if (c.thrift.hasError) throw c.thrift.getError
                ret
            })

        state { s =>
            val ret = try   { go(t)(s).get }
                      catch { case e => Result[A](Left(e)) }
            s -> ret
        }
    }

    private[this]
    def mkClient(host: String, port: Int, mgr: TAsyncClientManager): Client =
        new Client {
            lazy val thrift = new Cassandra.AsyncClient(factory, mgr, sock)
            def close() { sock.close() }

            private[this] lazy val sock    = new TNonblockingSocket(host, port)
            private[this] lazy val factory = new TProtocolFactory {
                def getProtocol(transport: TTransport): TProtocol =
                    new TBinaryProtocol(transport)
            }
        }

    private[this]
    def mkCassandraState(conf: CassandraConfig): CassandraState = {
        val mgr = new TAsyncClientManager
        val addrs = Stream.continually(conf.hosts).flatten.iterator
        val newClient = (_:Unit) => {
            val (h, p) = addrs.next
            mkClient(h, p, mgr)
        }

        val pool = createPool[Client](
          newClient
        , client => client.close()
        , conf.hosts.size
        , conf.connIdleTime
        , conf.maxConns
        )

        CassandraState(pool, Executors.newCachedThreadPool)
    }

    implicit
    def fnToCallable[A](f: => A): Callable[A] = new Callable[A] { def call = f }
}

object IO extends IO


// vim: set ts=4 sw=4 et:
