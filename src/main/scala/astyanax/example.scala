package astyanax

object Example extends App {

    import scala.concurrent.SyncVar
    import org.apache.cassandra.thrift._
    import Astyanax._


    runCassandra(CassandraConfig(), ("localhost", 9160)) { c =>
        val r = for {
          _ <- setKeyspace("Counters")
          y <- get("yyy", "ByHour_p_o_t", "bar", ConsistencyLevel.ONE)
          z <- get("zzz", "ByHour_p_o_t", "bar", ConsistencyLevel.ONE)
        } yield y :: z :: Nil

        println(runT(r)(c).get) // prints 'Result(Left(NotFoundException()))'
    }
}

// vim: set ts=4 sw=4 et:
