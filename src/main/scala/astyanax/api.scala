package astyanax

trait Api {

    import java.nio.ByteBuffer

    import scala.concurrent.SyncVar
    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._

    import org.apache.cassandra.thrift._
    import org.apache.thrift.async._

    import Types._
    import Internal._


    def login(username: String, password: String): Task[Unit] =
        mkTask(_._1.login(new AuthenticationRequest(Map(username -> password)), _))

    def setKeyspace(name: String): Task[Unit] =
        mkTask(_._1.set_keyspace(name, _))

    def get
        ( key: String
        , cf:  String
        , col: String
        , cl:  ConsistencyLevel
        )
    : Task[ColumnOrSuperColumn] =
        get(
          ByteBuffer.wrap(key.getBytes)
        , new ColumnPath(cf).setColumn(col.getBytes)
        , cl
        )

    def get
        ( key: ByteBuffer
        , cp:  ColumnPath
        , cl:  ConsistencyLevel
        )
    : Task[ColumnOrSuperColumn] =
        mkTask(_._1.get(key, cp, cl, _))

    def getSlice
        ( key: ByteBuffer
        , cp:  ColumnParent
        , sp:  SlicePredicate
        , cl:  ConsistencyLevel
        )
    : Task[Seq[ColumnOrSuperColumn]] =
        mkTask(_._1.get_slice(key, cp, sp, cl, _))

    def getCount
        ( key: ByteBuffer
        , cp:  ColumnParent
        , sp:  SlicePredicate
        , cl:  ConsistencyLevel
        )
    : Task[Int] =
        mkTask(_._1.get_count(key, cp, sp, cl, _))

    def multigetSlice
        ( keys: Seq[ByteBuffer]
        , cp:   ColumnParent
        , sp:   SlicePredicate
        , cl:   ConsistencyLevel
        )
    : Task[Map[ByteBuffer,Seq[ColumnOrSuperColumn]]] =
        mkTask(_._1.multiget_slice(keys, cp, sp, cl, _))

    def multigetCount
        ( keys: Seq[ByteBuffer]
        , cp:   ColumnParent
        , sp:   SlicePredicate
        , cl:   ConsistencyLevel
        )
    : Task[Map[ByteBuffer,Int]] =
        mkTask(_._1.multiget_count(keys, cp, sp, cl, _))

    def getRangeSlices
        ( cp: ColumnParent
        , sp: SlicePredicate
        , kr: KeyRange
        , cl: ConsistencyLevel
        )
    : Task[Seq[KeySlice]] =
        mkTask(_._1.get_range_slices(cp, sp, kr, cl, _))

    def getPagedSlice
        ( cf:       String
        , kr:       KeyRange
        , startCol: ByteBuffer
        , cl:       ConsistencyLevel
        )
    : Task[Seq[KeySlice]] =
        mkTask(_._1.get_paged_slice(cf, kr, startCol, cl, _))

    def getIndexedSlices
        ( cp:  ColumnParent
        , idx: IndexClause
        , sp:  SlicePredicate
        , cl:  ConsistencyLevel
        )
    : Task[Seq[KeySlice]] =
        mkTask(_._1.get_indexed_slices(cp, idx, sp, cl, _))

    def insert
        ( key: ByteBuffer
        , cp:  ColumnParent
        , col: Column
        , cl:  ConsistencyLevel
        )
    : Task[Unit] =
        mkTask(_._1.insert(key, cp, col, cl, _))

    def add
        ( key: ByteBuffer
        , cp:  ColumnParent
        , col: CounterColumn
        , cl:  ConsistencyLevel
        )
    : Task[Unit] =
        mkTask(_._1.add(key, cp, col, cl, _))

    def remove
        ( key: ByteBuffer
        , cp:  ColumnPath
        , ts:  Long
        , cl:  ConsistencyLevel
        )
    : Task[Unit] =
        mkTask(_._1.remove(key, cp, ts, cl, _))

    def removeCounter
        ( key: ByteBuffer
        , cp:  ColumnPath
        , cl:  ConsistencyLevel
        )
    : Task[Unit] =
        mkTask(_._1.remove_counter(key, cp, cl, _))

    def batchMutate
        ( mutations: Map[ByteBuffer,Map[String,Seq[Mutation]]]
        , cl:        ConsistencyLevel
        )
    : Task[Unit] =
        mkTask(_._1.batch_mutate(mutationMapToJava(mutations), cl, _))

    def truncate(cf: String): Task[Unit] =
        mkTask(_._1.truncate(cf, _))


    // "Meta-APIs"

    def describeSchemaVersions(): Task[Map[String,Seq[String]]] =
        mkTask(_._1.describe_schema_versions(_))

    def describeKeyspaces(): Task[Seq[KsDef]] =
        mkTask(_._1.describe_keyspaces(_))

    def describeClusterName(): Task[String] =
        mkTask(_._1.describe_cluster_name(_))

    def describeVersion(): Task[String] =
        mkTask(_._1.describe_version(_))

    def describeRing(keyspace: String): Task[Seq[TokenRange]] =
        mkTask(_._1.describe_ring(keyspace, _))

    def describePartitioner(): Task[String] =
        mkTask(_._1.describe_partitioner(_))

    def describeSnitch(): Task[String] =
        mkTask(_._1.describe_snitch(_))

    def describeKeyspace(keyspace: String): Task[KsDef] =
        mkTask(_._1.describe_keyspace(keyspace, _))

    // "system" operations

    def describeSplits
        ( cf:           String
        , startToken:   String
        , endToken:     String
        , keysPerSplit: Int
        )
    : Task[List[String]] =
        mkTask(_._1.describe_splits(cf, startToken, endToken, keysPerSplit, _))

    def systemAddColumnFamily(cfdef: CfDef): Task[String] =
        mkTask(_._1.system_add_column_family(cfdef, _))

    def systemDropColumnFamily(cf: String): Task[String] =
        mkTask(_._1.system_drop_column_family(cf, _))

    def systemAddKeyspace(ksdef: KsDef): Task[String] =
        mkTask(_._1.system_add_keyspace(ksdef, _))

    def systemDropKeyspace(ks: String): Task[String] =
        mkTask(_._1.system_drop_keyspace(ks, _))

    def systemUpdateKeyspace(ksdef: KsDef): Task[String] =
        mkTask(_._1.system_update_keyspace(ksdef, _))

    def systemUpdateColumnFamily(cfdef: CfDef): Task[String] =
        mkTask(_._1.system_update_column_family(cfdef, _))


    // CQL

    def executeCqlQuery(query: ByteBuffer, comp: Compression)
    : Task[CqlResult] =
        mkTask(_._1.execute_cql_query(query, comp, _))

    def prepareCqlQuery(query: ByteBuffer, comp: Compression)
    : Task[CqlPreparedResult] =
        mkTask(_._1.prepare_cql_query(query, comp, _))

    def executePreparedCqlQuery(id: Int, values: Seq[ByteBuffer])
    : Task[CqlResult] =
        mkTask(_._1.execute_prepared_cql_query(id, values, _))

    def setCqlVersion(v: String): Task[Unit] =
        mkTask(_._1.set_cql_version(v, _))

    private object Internal {

        import Cassandra.AsyncClient._
        type Login       = login_call
        type SetKeyspace = set_keyspace_call
        type Get         = get_call

        implicit def mkTask[A](f: (Client, SyncVar[Result[A]]) => Unit)
        : Task[A] =
            task(promise(f))

        implicit def callback[A, B](p: SyncVar[Result[B]])
        : AsyncMethodCallback[A] =
            new AsyncMethodCallback[A] {
                def onComplete(r: A) {
                  try { r match {
                      // damn you, thift authors: `getResult` is part of the
                      // interface of `TAsyncMethodCall`, but not declared in
                      // the base class, so generated methods (which exist even
                      // for `void` results) don't inherit
                      case x: Login =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: Get =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: SetKeyspace =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x => sys.error("wtf?")
                  }}
                  catch { case e => p put Left(e) }
                }
                def onError(e: Exception) { p put Left(e) }
            }


        // oh my
        import java.util.{ Map => JMap, List => JList }
        implicit def mutationMapToJava
            ( mm: Map[ByteBuffer,Map[String,Seq[Mutation]]]
            )
        : JMap[ByteBuffer,JMap[String,JList[Mutation]]] =
            mm.mapValues(_.mapValues(_.asJava).asJava).asJava
    }
}

object Api extends Api


// vim: set ts=4 sw=4 et:
