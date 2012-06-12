package astyanax

object Example extends App {

    import Astyanax._

    val Keyspace     = "Counters"
    val ColumnFamily = "ByHour_p_o_t"

    runCassandra(CassandraConfig(), ("localhost", 9160)) { c =>
        val r = for {
          _ <- setKeyspace(Keyspace)
          y <- get(Key("xxx"), ColumnPath(ColumnFamily, "bar"))
          z <- get(Key("zzz"), ColumnPath(ColumnFamily, "bar"))
        } yield y :: z :: Nil

        // prints 'Result(Left(NotFoundException()))'
        println(runT(r)(c).get)

        // perform a side-effect asynchronously
        def showCounterColumn(c: Col): Unit =
            c match { case CounterColumn(_, v) => println(v) }

        val s = for {
          _ <- setKeyspace(Keyspace)
          _ <- add(
                 Key("yyy")
               , ColumnParent(ColumnFamily)
               , CounterColumn("bar", 1)
               , All
               )
          y <- get(Key("yyy"), ColumnPath(ColumnFamily, "bar"))
          _ <- showCounterColumn(y)
          _ <- add(
                 Key("yyy")
               , ColumnParent(ColumnFamily)
               , CounterColumn("bar", -1)
               )
          z <- get(Key("yyy"), ColumnPath(ColumnFamily, "bar"))
          _ <- showCounterColumn(z)
        } yield y :: z :: Nil

        runT(s)(c).get // blocking anyway for completion
    }
}

// vim: set ts=4 sw=4 et:
