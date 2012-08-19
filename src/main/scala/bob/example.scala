package bob

object Example extends App {

    import java.util.concurrent.{ Executors, Future }

    import Bob._
    import thrift._
    import pool.HostConnectionPool._


    val Conf = CassandraConfig[AsyncClient, Future](
                 Seq("localhost" -> 9160)
               , RoundRobinPool
               , _ => Executors.newFixedThreadPool(
                          Runtime.getRuntime.availableProcessors)
               )
    val Cf   = CounterColumnFamily[String, Long](
                 "Counters"
               , "ByHour_p_o_t"
               , Utf8Codec
               , LongCodec
               )

    // a simple sequence of actions, intended to show how the computation stops
    // after the first failed action
    val r = for {
        _ <- setKeyspace(Cf)
        y <- get(Cf)("xxx", 1234L, Quorum)
        z <- get(Cf)("zzz", 1234L, Quorum)
    } yield y :: z :: Nil

    // an arbitrary side-effect
    def show[A](c: CounterColumn[A]): Unit =
        c match { case CounterColumn(n, v) => println(n + ":" + v) }

    val s = for {
        _ <- setKeyspace(Cf)
        _ <- add(Cf)("yyy", CounterColumn(1234L, 1), All)
        y <- get(Cf)("yyy", 1234L, Quorum)
        _ <- task(y.map(show))
        _ <- add(Cf)("yyy", CounterColumn(1234L, -1), Quorum)
        z <- get(Cf)("yyy", 1234L, Quorum)
        _ <- task(z.map(show))
    } yield y :: z :: Nil


    val res = runCassandra(Conf) { for (a <- r; b <- s) yield a :: b :: Nil }
    println("run cassandra monad:")
    println(res.get)

    // if your code isn't in monadic style, you can make a `Cassandra` holding
    // the (connection) state and pass it around
    val cass = newCassandra(Conf)
    println("run cassandra monad by passing state:")
    println(cass(r).get)
    cass(s).get.fold(
      e  => println("ERROR: " + e)
    , xs => xs.map(x => { println("x: " + x); x.map(show) })
    )

    println(runCassandra(Conf) { for(b <- s) yield b } get)
    releaseCassandra(cass) // must call this
}


// vim: set ts=4 sw=4 et:
