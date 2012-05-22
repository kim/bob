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
        mkTask(_._1.login(AuthReq(username, password), _))

    def setKeyspace(name: String): Task[Unit] =
        mkTask(_._1.set_keyspace(name, _))

    def get
        ( key: ByteBuffer
        , cp:  ColumnPath
        , cl:  ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[ColumnOrSuperColumn] =
        mkTask(_._1.get(key, cp, cl, _))

    def getSlice
        ( key: ByteBuffer
        , cp:  ColumnParent
        , sp:  SlicePredicate
        , cl:  ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Seq[ColumnOrSuperColumn]] =
        mkTask(_._1.get_slice(key, cp, sp, cl, _))

    def getCount
        ( key: ByteBuffer
        , cp:  ColumnParent
        , sp:  SlicePredicate
        , cl:  ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Int] =
        mkTask(_._1.get_count(key, cp, sp, cl, _))

    def multigetSlice
        ( keys: Seq[ByteBuffer]
        , cp:   ColumnParent
        , sp:   SlicePredicate
        , cl:   ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Map[ByteBuffer,Seq[ColumnOrSuperColumn]]] =
        mkTask(_._1.multiget_slice(keys, cp, sp, cl, _))

    def multigetCount
        ( keys: Seq[ByteBuffer]
        , cp:   ColumnParent
        , sp:   SlicePredicate
        , cl:   ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Map[ByteBuffer,Int]] =
        mkTask(_._1.multiget_count(keys, cp, sp, cl, _))

    def getRangeSlices
        ( cp: ColumnParent
        , sp: SlicePredicate
        , kr: KeyRange
        , cl: ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Seq[KeySlice]] =
        mkTask(_._1.get_range_slices(cp, sp, kr, cl, _))

    def getPagedSlice
        ( cf:       String
        , kr:       KeyRange
        , startCol: ByteBuffer
        , cl:       ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Seq[KeySlice]] =
        mkTask(_._1.get_paged_slice(cf, kr, startCol, cl, _))

    def insert
        ( key: ByteBuffer
        , cp:  ColumnParent
        , col: Column
        , cl:  ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Unit] =
        mkTask(_._1.insert(key, cp, col, cl, _))

    def add
        ( key: ByteBuffer
        , cp:  ColumnParent
        , col: CounterColumn
        , cl:  ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Unit] =
        mkTask(_._1.add(key, cp, col, cl, _))

    def remove
        ( key: ByteBuffer
        , cp:  ColumnPath
        , ts:  Long
        , cl:  ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Unit] =
        mkTask(_._1.remove(key, cp, ts, cl, _))

    def removeCounter
        ( key: ByteBuffer
        , cp:  ColumnPath
        , cl:  ConsistencyLevel = ConsistencyLevel.ONE
        )
    : Task[Unit] =
        mkTask(_._1.remove_counter(key, cp, cl, _))

    def batchMutate
        ( mutations: Map[ByteBuffer,Map[String,Seq[Mutation]]]
        , cl:        ConsistencyLevel = ConsistencyLevel.ONE
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

    // experimental  API for hadoop/parallel query support
    def describeSplits
        ( cf:           String
        , startToken:   String
        , endToken:     String
        , keysPerSplit: Int
        )
    : Task[List[String]] =
        mkTask(_._1.describe_splits(cf, startToken, endToken, keysPerSplit, _))

    // "system" operations

    type SchemaId = String

    def systemAddColumnFamily(cfdef: CfDef): Task[SchemaId] =
        mkTask(_._1.system_add_column_family(cfdef, _))

    def systemDropColumnFamily(cf: String): Task[SchemaId] =
        mkTask(_._1.system_drop_column_family(cf, _))

    def systemAddKeyspace(ksdef: KsDef): Task[SchemaId] =
        mkTask(_._1.system_add_keyspace(ksdef, _))

    def systemDropKeyspace(ks: String): Task[SchemaId] =
        mkTask(_._1.system_drop_keyspace(ks, _))

    def systemUpdateKeyspace(ksdef: KsDef): Task[SchemaId] =
        mkTask(_._1.system_update_keyspace(ksdef, _))

    def systemUpdateColumnFamily(cfdef: CfDef): Task[SchemaId] =
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
                      case x: login_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: get_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: set_keyspace_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: get_slice_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: get_count_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: multiget_slice_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: multiget_count_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: get_range_slices_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: get_paged_slice_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: insert_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: add_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: remove_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: remove_counter_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: batch_mutate_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: truncate_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: describe_schema_versions_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: describe_keyspaces_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: describe_cluster_name_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: describe_version_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: describe_partitioner_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: describe_snitch_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: describe_keyspace_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: describe_splits_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: system_add_column_family_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: system_drop_column_family_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: system_add_keyspace_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: system_drop_keyspace_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: system_update_keyspace_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: system_update_column_family_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: execute_cql_query_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: prepare_cql_query_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: execute_prepared_cql_query_call =>
                          p put Right(x.getResult.asInstanceOf[B])
                      case x: set_cql_version_call =>
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

        implicit def AuthReq(username: String, password: String)
        : AuthenticationRequest =
            new AuthenticationRequest(Map(username -> password))
    }
}

object Api extends Api


// vim: set ts=4 sw=4 et:
