package astyanax

trait Conversions {

    import java.nio.ByteBuffer
    import scala.collection.JavaConversions._
    import org.apache.cassandra.thrift


    type ConsistencyLevel = thrift.ConsistencyLevel
    type CL = ConsistencyLevel

    type Compression = thrift.Compression


    case class Key(value: Array[Byte])
    object Key {
        def apply(value: String): Key = Key(value.getBytes)
    }

    case class ColumnPath(cf: String, col: Array[Byte])
    object ColumnPath {
        def apply(cf: String, col: String): ColumnPath = ColumnPath(cf, col.getBytes)
    }

    case class SuperColumnPath(cf: String, scol: Array[Byte]) {
        def this(cf: String, scol: String) = this(cf, scol.getBytes)
    }

    case class SliceRange
        ( start:    Array[Byte]
        , finish:   Array[Byte]
        , reversed: Boolean = false
        , count:    Int = 100
        )
    {
        def this(start: String, finish: String, reversed: Boolean, count: Int) =
            this(start.getBytes, finish.getBytes, reversed, count)
    }

    case class SlicePredicate(cols: Seq[Array[Byte]], range: SliceRange)

    case class ColumnParent(cf: String, scol: Option[Array[Byte]])

    sealed trait Col
    case class Column
        ( name:      Array[Byte]
        , value:     Option[Array[Byte]]
        , timestamp: Option[Long]
        , ttl:       Option[Int]
        )
        extends Col
    case class SuperColumn
        ( name:    Array[Byte]
        , columns: Seq[Column]
        )
        extends Col
    case class CounterColumn
        ( name:  Array[Byte]
        , value: Long
        )
        extends Col
    case class CounterSuperColumn
        ( name:    Array[Byte]
        , columns: Seq[CounterColumn]
        )
        extends Col

    sealed trait Mutation
    case class Insert
        ( col: Col
        )
        extends Mutation
    case class Deletion
        ( timestamp: Long
        , supercol:  Option[Array[Byte]]
        , predicate: Option[SlicePredicate]
        )
        extends Mutation

    case class TokenRange
        ( start:           String
        , end:             String
        , endpoints:       Seq[String]
        , rpcEndpoints:    Option[Seq[String]]
        , endpointDetails: Option[Seq[EndpointDetails]]
        )
    case class EndpointDetails
        ( host:       String
        , datacenter: String
        , rack:       Option[String]
        )

    sealed trait CqlResult
    case object CqlVoidResult                   extends CqlResult
    case class CqlRowsResult(rows: Seq[CqlRow]) extends CqlResult
    case class CqlIntResult(n: Int)             extends CqlResult

    case class CqlRow(key: Key, cols: Seq[Column])

    case class CqlPreparedResult
        ( id:            Int
        , count:         Int
        , variableTypes: Option[Seq[String]]
        )

    case class CqlMetadata
        ( nameTypes:        Map[Array[Byte],String]
        , valueTypes:       Map[Array[Byte],String]
        , defaultNameType:  String
        , defaultValueType: String
        )



    implicit def keyToByteBuffer(k: Key): ByteBuffer =
        ByteBuffer.wrap(k.value)

    implicit def columnPathToThrift(cp: ColumnPath): thrift.ColumnPath =
        new thrift.ColumnPath(cp.cf).setColumn(cp.col)

    implicit def superColumnPathToThrift(scp: SuperColumnPath)
    : thrift.ColumnPath =
        new thrift.ColumnPath(scp.cf).setSuper_column(scp.scol)

    implicit def sliceRangeToThrift(sr: SliceRange): thrift.SliceRange =
        new thrift.SliceRange(
          ByteBuffer.wrap(sr.start)
        , ByteBuffer.wrap(sr.finish)
        , sr.reversed
        , sr.count
        )

    implicit def slicePredicateToThrift(sp: SlicePredicate)
    : thrift.SlicePredicate =
        new thrift.SlicePredicate()
                  .setColumn_names(sp.cols.map(ByteBuffer.wrap))
                  .setSlice_range(sp.range)

    implicit def columnParentToThrift(cp: ColumnParent)
    : thrift.ColumnParent =
        cp.scol.toList.foldLeft(new thrift.ColumnParent(cp.cf))(
            (a,s) => a.setSuper_column(s))

    implicit def columnToThrift(col: Column): thrift.Column = {
        val r = new thrift.Column(ByteBuffer.wrap(col.name))
        col.value.map(r.setValue)
        col.timestamp.map(r.setTimestamp)
        col.ttl.map(r.setTtl)
        r
    }

    implicit def columnFromThrift(x: thrift.Column): Column =
        Column(x.name, Option(x.value), Option(x.timestamp), Option(x.ttl))

    implicit def superColumnToThrift(scol: SuperColumn)
    : thrift.SuperColumn =
        new thrift.SuperColumn(
          ByteBuffer.wrap(scol.name)
        , scol.columns.map(columnToThrift)
        )

    implicit def superColumnFromThrift(x: thrift.SuperColumn)
    : SuperColumn =
        SuperColumn(x.name, x.columns.map(columnFromThrift))

    implicit def counterColumnToThrift(cc: CounterColumn)
    : thrift.CounterColumn =
        new thrift.CounterColumn(ByteBuffer.wrap(cc.name), cc.value)

    implicit def counterColumnFromThrift(x: thrift.CounterColumn)
    : CounterColumn =
        CounterColumn(x.name, x.value)

    implicit def counterSuperColumnToThrift(csc: CounterSuperColumn)
    : thrift.CounterSuperColumn =
        new thrift.CounterSuperColumn(
          ByteBuffer.wrap(csc.name)
        , csc.columns.map(counterColumnToThrift)
        )

    implicit def counterSuperColumnFromThrift(x: thrift.CounterSuperColumn)
    : CounterSuperColumn =
        CounterSuperColumn(x.name, x.columns.map(counterColumnFromThrift))

    implicit def colToThrift(col: Col): thrift.ColumnOrSuperColumn = {
        val r = new thrift.ColumnOrSuperColumn
        col match {
            case x: Column             => r.setColumn(x)
            case x: SuperColumn        => r.setSuper_column(x)
            case x: CounterColumn      => r.setCounter_column(x)
            case x: CounterSuperColumn => r.setCounter_super_column(x)
        }
        r
    }

    implicit def colFromThrift(c: thrift.ColumnOrSuperColumn): Col =
        Option(c.column).map(columnFromThrift)
        .orElse(Option(c.super_column).map(superColumnFromThrift))
        .orElse(Option(c.counter_column).map(counterColumnFromThrift))
        .orElse(Option(c.counter_super_column).map(counterSuperColumnFromThrift))
        .get

    implicit def byteBufferToByteArray(bb: ByteBuffer): Array[Byte] =
        if (bb.hasArray)
            bb.array
        else {
            val ba = new Array[Byte](bb.remaining)
            bb.get(ba)
            ba
        }

    implicit def mutationToThrift(m: Mutation): thrift.Mutation =
        m match {
            case Insert(x) =>
                new thrift.Mutation().setColumn_or_supercolumn(x)
            case Deletion(ts, sc, sp) => {
                val del = new thrift.Deletion().setTimestamp(ts)
                sc.map(del.setSuper_column)
                sp.map(p => del.setPredicate(slicePredicateToThrift(p)))

                new thrift.Mutation().setDeletion(del)
            }
        }

    implicit def tokenRangeToThrift(t: TokenRange): thrift.TokenRange = {
        val r = new thrift.TokenRange(t.start, t.end, t.endpoints)
        t.rpcEndpoints.map(x => r.setRpc_endpoints(x))
        t.endpointDetails.map(x =>
            r.setEndpoint_details(x.map(endpointDetailsToThrift)))
        r
    }

    implicit def endpointDetailsToThrift(d: EndpointDetails)
    : thrift.EndpointDetails = {
        val r = new thrift.EndpointDetails(d.host, d.datacenter)
        d.rack.map(r.setRack)
        r
    }

}

object Conversions extends Conversions


// vim: set ts=4 sw=4 et: