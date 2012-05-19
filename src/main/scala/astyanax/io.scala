package astyanax

trait IO {

    import java.util.concurrent.{ ArrayBlockingQueue
                                , BlockingQueue
                                , Callable
                                , Executors
                                , ExecutorService
                                , Future
                                }

    import scala.collection.JavaConversions._

    import org.apache.cassandra.thrift._
    import org.apache.thrift.async._
    import org.apache.thrift.protocol._
    import org.apache.thrift.transport._

    import Types._


    case class CassandraConfig()
    case class CassandraRunner
        ( cs: BlockingQueue[Client]
        , e:  ExecutorService
        )
    {
        def submit[A](t: Task[A]): Future[Result[A]] =
            e.submit(t(cs.take).get match { case (a, c) => cs.add(c); a })
    }

    // TODO: `runCassandra` should actually run a cassandra monad (which threads
    // state such as the `ExecutorService` and the client queue), rather than
    // being a simplistic combinator
    def runCassandra(conf: CassandraConfig, hosts: (String, Int)*)
                    (f: CassandraRunner => Unit)
    {
        val mgr = new TAsyncClientManager
        val pool = new ArrayBlockingQueue[Client](hosts.size)
        hosts.foreach { case (h, p) =>
            pool.add({
                val sock = new TNonblockingSocket(h, p)
                (new Cassandra.AsyncClient(factory, mgr, sock), sock)
            })
        }
        val exec = Executors.newCachedThreadPool

        f(CassandraRunner(pool, exec))

        exec.shutdownNow()
        pool.foreach { case (_, sock) => sock.close() }
        mgr.stop()
    }

    // TODO: likewise, `runT` should run in the cassandra monad
    def runT[A](t: Task[A])(implicit R: CassandraRunner): Future[Result[A]] =
        R.submit(t)

    private
    val factory = new TProtocolFactory {
        def getProtocol(transport: TTransport): TProtocol =
              new TBinaryProtocol(transport)
    }

    implicit
    def fnToCallable[A](f: => A): Callable[A] = new Callable[A] { def call = f }
}

object IO extends IO


// vim: set ts=4 sw=4 et:
