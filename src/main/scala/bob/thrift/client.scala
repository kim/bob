package bob.thrift


trait ClientInstances {

    import org.apache.thrift.async.TAsyncClientManager
    import org.apache.thrift.protocol.{ TBinaryProtocol
                                      , TProtocol
                                      , TProtocolFactory
                                      }
    import org.apache.thrift.transport.{ TNonblockingSocket, TTransport }

    import bob.Bob._


    type ThriftClient = Client[AsyncClient]

    def ThriftClient(host: String, port: Int, mgr: TAsyncClientManager)
    : ThriftClient = new ThriftClient {

        def runWith[B](f: AsyncClient => B): B = {
            if (thrift.hasError)
                throw thrift.getError
            val r = f(thrift)
            if (thrift.hasError)
                throw thrift.getError
            r
        }

        def close() { sock.close() }

        private[this]
        lazy val thrift = new AsyncClient(factory, mgr, sock)

        private[this]
        lazy val sock = new TNonblockingSocket(host, port)

        private[this]
        lazy val factory = new TProtocolFactory {
            def getProtocol(transport: TTransport): TProtocol =
                new TBinaryProtocol(transport)
        }
    }
}
