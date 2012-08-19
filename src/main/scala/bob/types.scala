package bob

trait Types {

    import Bob._


    trait Client[A] {
        def runWith[B](f: A => B): B
        def close()
    }

    sealed trait ConsistencyLevel
    case object AnyConsistency extends ConsistencyLevel
    case object One            extends ConsistencyLevel
    case object Two            extends ConsistencyLevel
    case object Three          extends ConsistencyLevel
    case object Quorum         extends ConsistencyLevel
    case object LocalQuorum    extends ConsistencyLevel
    case object EachQuorum     extends ConsistencyLevel
    case object All            extends ConsistencyLevel

    sealed trait Compression
    case object Gzip          extends Compression
    case object NoCompression extends Compression

    type Keyspace = String

    sealed abstract class CF
        ( val ks:   Keyspace
        , val name: String
        )
    case class ColumnFamily[Key, Name, Value]
        ( _ks:       Keyspace
        , _name:     String
        , keyCodec:  Codec[Key]
        , nameCodec: Codec[Name]
        , valCodec:  Codec[Value]
        )
        extends CF(_ks, _name)
    case class SuperColumnFamily[Key, Name, SubName, SubValue]
        ( _ks:          Keyspace
        , _name:        String
        , keyCodec:     Codec[Key]
        , nameCodec:    Codec[Name]
        , colNameCodec: Codec[SubName]
        , colValCodec:  Codec[SubValue]
        )
        extends CF(_ks, _name)
    case class CounterColumnFamily[Key, Name]
        ( _ks:        Keyspace
        , _name:      String
        , keyCodec:   Codec[Key]
        , nameCodec:  Codec[Name]
        )
        extends CF(_ks, _name)
    case class SuperCounterColumnFamily[Key, Name, SubName]
        ( _ks:          Keyspace
        , _name:        String
        , keyCodec:     Codec[Key]
        , nameCodec:    Codec[Name]
        , colNameCodec: Codec[SubName]
        )
        extends CF(_ks, _name)


    case class Key[A]
        ( value: A
        )

    case class ColumnPath[+A, +B]
        ( cf:   String
        , col:  Option[A]
        , scol: Option[B]
        )

    case class SliceRange[A]
        ( start:    A
        , finish:   A
        , reversed: Boolean = false
        , count:    Int     = 100
        )

    case class SlicePredicate[A]
        ( cols:  Seq[A]
        , range: SliceRange[A]
        )

    case class ColumnParent[A]
        ( cf:   String
        , scol: Option[A] = None
        )

    sealed trait Col
    case class Column[A, B]
        ( name:      A
        , value:     Option[B]    = None
        , timestamp: Option[Long] = None
        , ttl:       Option[Int]  = None
        )
        extends Col
    case class SuperColumn[A, B, C]
        ( name:    A
        , columns: Seq[Column[B, C]]
        )
        extends Col
    case class CounterColumn[A]
        ( name:  A
        , value: Long
        )
        extends Col
    case class CounterSuperColumn[A, B]
        ( name:    A
        , columns: Seq[CounterColumn[B]]
        )
        extends Col

    case class TokenRange
        ( start:           String
        , end:             String
        , endpoints:       Seq[String]
        , rpcEndpoints:    Option[Seq[String]]          = None
        , endpointDetails: Option[Seq[EndpointDetails]] = None
        )
    case class EndpointDetails
        ( host:       String
        , datacenter: String
        , rack:       Option[String] = None
        )

    sealed trait CqlResult
    case object CqlVoidResult extends CqlResult

    case class CqlRowsResult[A, B, C]
        ( rows: Seq[CqlRow[A, B, C]]
        )
        extends CqlResult

    case class CqlIntResult(n: Int) extends CqlResult

    case class CqlRow[A, B, C]
        ( key:  Key[A]
        , cols: Seq[Column[B, C]]
        )

    case class CqlPreparedResult
        ( id:            Int
        , count:         Int
        , variableTypes: Option[Seq[String]] = None
        )

    case class CqlMetadata
        ( nameTypes:        Map[Array[Byte],String]
        , valueTypes:       Map[Array[Byte],String]
        , defaultNameType:  String
        , defaultValueType: String
        )


//    object Conversions {

        import java.nio.ByteBuffer
        import scala.collection.mutable.{ HashMap, ListBuffer }
        import scala.collection.JavaConversions._
        import org.apache.cassandra.thrift


        object Key {
            def convert[A](k: Key[A])(implicit codec: Codec[A]): ByteBuffer =
                codec.encode(k.value)

            def convert[A](b: ByteBuffer)(implicit codec: Codec[A]): Key[A] =
                Key(codec.decode(b))
        }

        object Column {
            def convert[A, B]
                ( col: Column[A, B]
                )
                (implicit nameCodec: Codec[A], valCodec:  Codec[B])
            : thrift.Column = {
                val r = new thrift.Column(nameCodec.encode(col.name))
                col.value.map(v => r.setValue(valCodec.encode(v)))
                col.timestamp.map(r.setTimestamp)
                col.ttl.map(r.setTtl)
                r
            }

            def convert[A, B]
                ( col: thrift.Column
                )
                (implicit nameCodec: Codec[A], valCodec:  Codec[B])
            : Column[A, B] =
                Column(
                  nameCodec.decode(col.name)
                , Option(col.value).map(valCodec.decode)
                , Option(col.timestamp)
                , Option(col.ttl)
                )

            def convertCosc[A, B]
                ( cosc: thrift.ColumnOrSuperColumn
                )
                (implicit nameCodec: Codec[A], valCodec:  Codec[B])
            : Option[Column[A, B]] =
                Option(cosc.column).map(c =>
                    Column.convert(c)(nameCodec, valCodec))

            def convertCosc[A, B]
                ( col: Column[A, B]
                )
                (implicit nameCodec: Codec[A], valCodec:  Codec[B])
            : thrift.ColumnOrSuperColumn =
                new thrift.ColumnOrSuperColumn()
                    .setColumn(Column.convert(col)(nameCodec, valCodec))
        }

        object SuperColumn {
            def convert[A, B, C]
                ( scol: SuperColumn[A, B, C]
                )
                (implicit nameCodec:    Codec[A]
                        , colNameCodec: Codec[B]
                        , colValCodec:  Codec[C]
                )
            : thrift.SuperColumn =
                new thrift.SuperColumn(
                  nameCodec.encode(scol.name)
                , scol.columns.map(c => Column.convert(c)(colNameCodec, colValCodec))
                )

            def convert[A, B, C]
                ( scol: thrift.SuperColumn
                )
                (implicit nameCodec:    Codec[A]
                        , colNameCodec: Codec[B]
                        , colValCodec:  Codec[C]
                )
            : SuperColumn[A, B, C] =
                SuperColumn(
                  nameCodec.decode(scol.name)
                , scol.columns.map(c => Column.convert(c)(colNameCodec, colValCodec))
                )

            def convertCosc[A, B, C]
                ( cosc: thrift.ColumnOrSuperColumn
                )
                (implicit nameCodec:    Codec[A]
                        , colNameCodec: Codec[B]
                        , colValCodec:  Codec[C]
                )
            : Option[SuperColumn[A, B, C]] =
                Option(cosc.super_column).map(sc =>
                    SuperColumn.convert(sc)(nameCodec, colNameCodec, colValCodec))

            def convertCosc[A, B, C]
                ( scol: SuperColumn[A, B, C]
                )
                (implicit nameCodec:    Codec[A]
                        , colNameCodec: Codec[B]
                        , colValCodec:  Codec[C]
                )
            : thrift.ColumnOrSuperColumn =
                new thrift.ColumnOrSuperColumn().setSuper_column(
                    SuperColumn.convert(scol)(nameCodec, colNameCodec, colValCodec)
                )
        }

        object CounterColumn {
            def convert[A]
                ( ccol: CounterColumn[A]
                )
                (implicit codec: Codec[A])
            : thrift.CounterColumn =
                new thrift.CounterColumn(
                  codec.encode(ccol.name)
                , ccol.value
                )

            def convert[A]
                ( ccol: thrift.CounterColumn
                )
                (implicit codec: Codec[A])
            : CounterColumn[A] =
                CounterColumn(
                  codec.decode(ccol.name)
                , ccol.value
                )

            def convertCosc[A]
                ( cosc: thrift.ColumnOrSuperColumn
                )
                (implicit codec: Codec[A])
            : Option[CounterColumn[A]] =
                Option(cosc.counter_column).map(cc =>
                    CounterColumn.convert(cc)(codec))

            def convertCosc[A]
                ( ccol: CounterColumn[A]
                )
                (implicit codec: Codec[A])
            : thrift.ColumnOrSuperColumn =
                new thrift.ColumnOrSuperColumn()
                    .setCounter_column(CounterColumn.convert(ccol)(codec))
            }

        object CounterSuperColumn {
            def convert[A, B]
                ( csc: CounterSuperColumn[A, B]
                )
                (implicit codec: Codec[A], colCodec: Codec[B])
            : thrift.CounterSuperColumn =
                new thrift.CounterSuperColumn(
                  codec.encode(csc.name)
                , csc.columns.map(c => CounterColumn.convert(c)(colCodec))
                )

            def convert[A, B]
                ( csc: thrift.CounterSuperColumn
                )
                (implicit codec: Codec[A], colCodec: Codec[B])
            : CounterSuperColumn[A, B] =
                CounterSuperColumn(
                  codec.decode(csc.name)
                , csc.columns.map(c => CounterColumn.convert(c)(colCodec))
                )

            def convertCosc[A, B]
                ( cosc: thrift.ColumnOrSuperColumn
                )
                (implicit codec: Codec[A], colCodec: Codec[B])
            : Option[CounterSuperColumn[A, B]] =
                Option(cosc.counter_super_column).map(csc =>
                    CounterSuperColumn.convert(csc)(codec, colCodec))

            def convertCosc[A, B]
                ( csc: CounterSuperColumn[A, B]
                )
                (implicit codec: Codec[A], colCodec: Codec[B])
            : thrift.ColumnOrSuperColumn =
                new thrift.ColumnOrSuperColumn().setCounter_super_column(
                    CounterSuperColumn.convert(csc)(codec, colCodec)
                )
        }

        object ColumnPath {
            def convert[A, B]
                ( cp: ColumnPath[A, B]
                )
                (implicit colCodec: Codec[A], scolCodec: Codec[B])
            : thrift.ColumnPath = {
                val ret = new thrift.ColumnPath(cp.cf)
                cp.col.map(c => ret.setColumn(colCodec.encode(c)))
                cp.scol.map(s => ret.setSuper_column(scolCodec.encode(s)))
                ret
            }
        }

        object SliceRange {
            def convert[A]
                ( sr: SliceRange[A]
                )
                (implicit codec: Codec[A])
            : thrift.SliceRange =
                new thrift.SliceRange(
                  codec.encode(sr.start)
                , codec.encode(sr.finish)
                , sr.reversed
                , sr.count
                )
        }

        object SlicePredicate {
            def convert[A]
                ( sp: SlicePredicate[A]
                )
                (implicit codec: Codec[A])
            : thrift.SlicePredicate =
                new thrift.SlicePredicate()
                          .setColumn_names(sp.cols.map(codec.encode))
                          .setSlice_range(SliceRange.convert(sp.range)(codec))
        }

        object ColumnParent {
            def convert[A]
                ( cp: ColumnParent[A]
                )
                (implicit codec: Codec[A])
            : thrift.ColumnParent =
                (new thrift.ColumnParent(cp.cf) /: cp.scol.toList) {
                    (a,s) => a.setSuper_column(codec.encode(s))
                }
        }

        type MutationMap =
            HashMap[ByteBuffer, HashMap[String, ListBuffer[thrift.Mutation]]]

        case class BatchMutation
            ( mutations: MutationMap
            )
        object BatchMutation {
            lazy val empty = BatchMutation(HashMap.empty)

            def freeze(bm: BatchMutation)
            : Map[ByteBuffer, Map[String, Seq[thrift.Mutation]]] =
                bm.mutations.map { case (k, m) =>
                    k -> m.map { case (cf, l) => cf -> l.toSeq }.toMap
                }.toMap

            def insert[K, N, V]
                ( bm:   BatchMutation
                )
                ( key:  K
                , cf:   ColumnFamily[K, N, V]
                , col:  Column[N, V]
                )
                (implicit keyCodec:   Codec[K]
                        , nameCodec:  Codec[N]
                        , valCodec:   Codec[V]
                )
            : BatchMutation = {
                update(
                  bm.mutations
                , keyCodec.encode(key)
                , cf.name
                , new thrift.Mutation().setColumn_or_supercolumn(
                    Column.convertCosc(col)(nameCodec, valCodec)
                  )
                )

                bm
            }

            def insert[K, N, NN, VV]
                ( bm:   BatchMutation
                )
                ( key:  K
                , cf:   SuperColumnFamily[K, N, NN, VV]
                , col:  SuperColumn[N, NN, VV]
                )
                (implicit keyCodec:     Codec[K]
                        , nameCodec:    Codec[N]
                        , colNameCodec: Codec[NN]
                        , colValCodec:  Codec[VV]
                )
            : BatchMutation = {
                update(
                  bm.mutations
                , keyCodec.encode(key)
                , cf.name
                , new thrift.Mutation().setColumn_or_supercolumn(
                    SuperColumn.convertCosc(col)( nameCodec
                                                , colNameCodec
                                                , colValCodec
                                                )
                  )
                )

                bm
            }

            def insert[K, N]
                ( bm:   BatchMutation
                )
                ( key:  K
                , cf:   CounterColumnFamily[K, N]
                , col:  CounterColumn[N]
                )
                (implicit keyCodec:   Codec[K]
                        , nameCodec:  Codec[N]
                )
            : BatchMutation = {
                update(
                  bm.mutations
                , keyCodec.encode(key)
                , cf.name
                , new thrift.Mutation().setColumn_or_supercolumn(
                    CounterColumn.convertCosc(col)(nameCodec)
                  )
                )

                bm
            }

            def insert[K, N, NN]
                ( bm:   BatchMutation
                )
                ( key:  K
                , cf:   SuperCounterColumnFamily[K, N, NN]
                , col:  CounterSuperColumn[N, NN]
                )
                (implicit keyCodec:     Codec[K]
                        , nameCodec:    Codec[N]
                        , colNameCodec: Codec[NN]
                )
            : BatchMutation = {
                update(
                  bm.mutations
                , keyCodec.encode(key)
                , cf.name
                , new thrift.Mutation().setColumn_or_supercolumn(
                    CounterSuperColumn.convertCosc(col)(nameCodec, colNameCodec)
                  )
                )

                bm
            }

            def delete[K, N, V]
                ( bm:   BatchMutation
                )
                ( key:  K
                , cf:   ColumnFamily[K, N, V]
                , pred: Option[SlicePredicate[N]]
                )
                (implicit keyCodec:   Codec[K]
                        , nameCodec:  Codec[N]
                )
            : BatchMutation = {
                val del = new thrift.Deletion()
                              .setTimestamp(System.currentTimeMillis * 1000)
                pred map ( p =>
                    del.setPredicate(SlicePredicate.convert(p)(nameCodec))
                )

                update(
                  bm.mutations
                , keyCodec.encode(key)
                , cf.name
                , new thrift.Mutation().setDeletion(del)
                )

                bm
            }

            def delete[K, N, NN, VV]
                ( bm:   BatchMutation
                )
                ( key:  K
                , cf:   SuperColumnFamily[K, N, NN, VV]
                , pred: Option[SlicePredicate[N]]
                , col:  Option[SuperColumn[N, NN, VV]]
                )
                (implicit keyCodec:     Codec[K]
                        , nameCodec:    Codec[N]
                        , colNameCodec: Codec[NN]
                        , colValCodec:  Codec[VV]
                )
            : BatchMutation = {
                val del = new thrift.Deletion()
                              .setTimestamp(System.currentTimeMillis * 1000)
                pred map ( p =>
                    del.setPredicate(SlicePredicate.convert(p)(nameCodec))
                )
                col map ( c =>
                    del.setSuper_column(nameCodec.encode(c.name))
                )

                update(
                  bm.mutations
                , keyCodec.encode(key)
                , cf.name
                , new thrift.Mutation().setDeletion(del)
                )

                bm
            }

            def delete[K, N]
                ( bm:   BatchMutation
                )
                ( key:  K
                , cf:   CounterColumnFamily[K, N]
                , pred: Option[SlicePredicate[N]]
                )
                (implicit keyCodec:   Codec[K]
                        , nameCodec:  Codec[N]
                )
            : BatchMutation = {
                val del = new thrift.Deletion()
                              .setTimestamp(System.currentTimeMillis * 1000)
                pred map ( p =>
                    del.setPredicate(SlicePredicate.convert(p)(nameCodec))
                )

                update(
                  bm.mutations
                , keyCodec.encode(key)
                , cf.name
                , new thrift.Mutation().setDeletion(del)
                )

                bm
            }

            def delete[K, N, NN]
                ( bm:   BatchMutation
                )
                ( key:  K
                , cf:   SuperCounterColumnFamily[K, N, NN]
                , pred: Option[SlicePredicate[N]]
                , col:  Option[CounterSuperColumn[N, NN]]
                )
                (implicit keyCodec:   Codec[K]
                        , nameCodec:  Codec[N]
                )
            : BatchMutation = {
                val del = new thrift.Deletion()
                              .setTimestamp(System.currentTimeMillis * 1000)
                pred map ( p =>
                    del.setPredicate(SlicePredicate.convert(p)(nameCodec))
                )
                col map ( c =>
                    del.setSuper_column(nameCodec.encode(c.name))
                )

                update(
                  bm.mutations
                , keyCodec.encode(key)
                , cf.name
                , new thrift.Mutation().setDeletion(del)
                )

                bm
            }

            private[this]
            def update
                ( mm: MutationMap
                , k:  ByteBuffer
                , cf: String
                , m:  thrift.Mutation
                )
            : Unit =
                mm.getOrElseUpdate(
                  k
                , HashMap.empty[String, ListBuffer[thrift.Mutation]]
                ).getOrElseUpdate(
                  cf
                , ListBuffer.empty[thrift.Mutation]
                ) += m

            /*
                val Deletion(ts, sc, sp) = d
                val del = new thrift.Deletion().setTimestamp(ts)
                sc.map(c => del.setSuper_column(codec.encode(c)))
                sp.map(p => del.setPredicate(SlicePredicate.convert(p)(codec)))

                new thrift.Mutation().setDeletion(del)

            case class Deletion[A]
                ( timestamp: Long
                , supercol:  Option[A] = None
                , predicate: Option[SlicePredicate[A]] = None
                )
            */
        }


        //
        // implicits
        //

        implicit
        def consistencyLevelToThrift(cl: ConsistencyLevel)
        : thrift.ConsistencyLevel =
            cl match {
                case AnyConsistency => thrift.ConsistencyLevel.ANY
                case One            => thrift.ConsistencyLevel.ONE
                case Two            => thrift.ConsistencyLevel.TWO
                case Three          => thrift.ConsistencyLevel.THREE
                case Quorum         => thrift.ConsistencyLevel.QUORUM
                case LocalQuorum    => thrift.ConsistencyLevel.LOCAL_QUORUM
                case EachQuorum     => thrift.ConsistencyLevel.EACH_QUORUM
                case All            => thrift.ConsistencyLevel.ALL
            }

        implicit
        def compressionToThrift(c: Compression): thrift.Compression =
            c match {
                case Gzip          => thrift.Compression.GZIP
                case NoCompression => thrift.Compression.NONE
            }

        implicit
        def tokenRangeToThrift(t: TokenRange): thrift.TokenRange = {
            val r = new thrift.TokenRange(t.start, t.end, t.endpoints)
            t.rpcEndpoints.map(x => r.setRpc_endpoints(x))
            t.endpointDetails.map(x =>
                r.setEndpoint_details(x.map(endpointDetailsToThrift)))
            r
        }

        implicit
        def endpointDetailsToThrift(d: EndpointDetails)
        : thrift.EndpointDetails = {
            val r = new thrift.EndpointDetails(d.host, d.datacenter)
            d.rack.map(r.setRack)
            r
        }
//    }
}

object Types extends Types


// vim: set ts=4 sw=4 et:
