package astyanax

object Example extends App {

    import Astyanax._

    val Keyspace     = "Counters"
    val ColumnFamily = "ByHour_p_o_t"

    val conf = CassandraConfig(Seq("localhost" -> 9160))

    val r = for {
        _ <- setKeyspace(Keyspace)
        y <- get(Key("xxx"), ColumnPath(ColumnFamily, "bar"))
        z <- get(Key("zzz"), ColumnPath(ColumnFamily, "bar"))
    } yield y :: z :: Nil


    // perform a side-effect asynchronously

    // this is our side-effect, which is automatically lifted into the Task
    // monad through an implicit conversion
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


    val res = runCassandra(conf) { for(a <- r; b <- s) yield a :: b :: Nil }

    println(res)
}


// vim: set ts=4 sw=4 et:
