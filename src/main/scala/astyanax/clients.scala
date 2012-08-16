package astyanax

trait Clients {

    import org.apache.cassandra.thrift.{ Cassandra => Thrift }
    import org.apache.thrift.async.TAsyncClientManager
    import org.apache.thrift.protocol.{ TBinaryProtocol
                                      , TProtocol
                                      , TProtocolFactory
                                      }
    import org.apache.thrift.transport.{ TNonblockingSocket, TTransport }


    trait Client[A] {
        def runWith[B](f: A => B): B
        def close()
    }

    type ThriftClient = Client[Thrift.AsyncClient]

    def ThriftClient(host: String, port: Int, mgr: TAsyncClientManager)
    : ThriftClient = new ThriftClient {

        def runWith[B](f: Thrift.AsyncClient => B): B = {
            if (thrift.hasError)
                throw thrift.getError
            val r = f(thrift)
            if (thrift.hasError)
                throw thrift.getError
            r
        }

        def close() { sock.close() }

        private[this]
        lazy val thrift = new Thrift.AsyncClient(factory, mgr, sock)

        private[this]
        lazy val sock = new TNonblockingSocket(host, port)

        private[this]
        lazy val factory = new TProtocolFactory {
            def getProtocol(transport: TTransport): TProtocol =
                new TBinaryProtocol(transport)
        }
    }
}

object Clients extends Clients


// vim: set ts=4 sw=4 et:
