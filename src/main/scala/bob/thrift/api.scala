package bob.thrift

trait Api {

    import java.nio.ByteBuffer
    import scala.collection.mutable.HashMap

    import bob.Bob._


    def setKeyspace(cf: CF): Task[ThriftClient, Unit] =
        Thrift.setKeyspace(cf.ks)

    def get[K, N, V]
        ( cf:  ColumnFamily[K, N, V]
        )
        ( key:  K
        , path: N
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Option[Column[N, V]]] = {
        val ColumnFamily(_, name, keyCodec, nameCodec, valCodec) = cf
        Thrift.get(
          Key.convert(Key(key))(keyCodec)
        , ColumnPath.convert(ColumnPath(name, Some(path), None))(nameCodec, null)
        , cl
        ).map(Column.convertCosc(_)(nameCodec, valCodec))
    }

    def get[K, N, NN, VV]
        ( cf:  SuperColumnFamily[K, N, NN, VV]
        )
        ( key:  K
        , path: N
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Option[SuperColumn[N, NN, VV]]] = {
        val SuperColumnFamily(_, name, keyCodec, nameCodec, colNameCodec, colValCodec) = cf
        Thrift.get(
          Key.convert(Key(key))(keyCodec)
        , ColumnPath.convert(ColumnPath(name, None, Some(path)))(null, nameCodec)
        , cl
        ).map(SuperColumn.convertCosc(_)(nameCodec, colNameCodec, colValCodec))
    }

    def get[K, N]
        ( cf:  CounterColumnFamily[K, N]
        )
        ( key:  K
        , path: N
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Option[CounterColumn[N]]] = {
        val CounterColumnFamily(_, name, keyCodec, nameCodec) = cf
        Thrift.get(
          Key.convert(Key(key))(keyCodec)
        , ColumnPath.convert(ColumnPath(name, Some(path), None))(nameCodec, null)
        , cl
        ).map(CounterColumn.convertCosc(_)(nameCodec))
    }

    def get[K, N, NN]
        ( cf:  SuperCounterColumnFamily[K, N, NN]
        )
        ( key:  K
        , path: N
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Option[CounterSuperColumn[N, NN]]] = {
        val SuperCounterColumnFamily(_, name, keyCodec, nameCodec, colNameCodec) = cf
        Thrift.get(
          Key.convert(Key(key))(keyCodec)
        , ColumnPath.convert(ColumnPath(name, None, Some(path)))(null, nameCodec)
        , cl
        ).map(CounterSuperColumn.convertCosc(_)(nameCodec, colNameCodec))
    }

    def add[K, N]
        ( cf:  CounterColumnFamily[K, N]
        )
        ( key: K
        , col: CounterColumn[N]
        , cl:  ConsistencyLevel
        )
    : Task[ThriftClient, Unit] = {
        val CounterColumnFamily(_, name, keyCodec, nameCodec) = cf
        Thrift.add(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , CounterColumn.convert(col)(nameCodec)
        , cl
        )
    }

    def add[K, N, NN]
        ( cf:  SuperCounterColumnFamily[K, N, NN]
        )
        ( key:  K
        , scol: N
        , col:  CounterColumn[NN]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Unit] = {
        val SuperCounterColumnFamily(_, name, keyCodec, nameCodec, colNameCodec) = cf
        Thrift.add(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name, Some(scol)))(nameCodec)
        , CounterColumn.convert(col)(colNameCodec)
        , cl
        )
    }

    def insert[K, N, V]
        ( cf: ColumnFamily[K, N, V]
        )
        ( key: K
        , col: Column[N, V]
        , cl:  ConsistencyLevel
        )
    : Task[ThriftClient, Unit] = {
        val ColumnFamily(_, name, keyCodec, nameCodec, valCodec) = cf
        Thrift.insert(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , Column.convert(col)(nameCodec, valCodec)
        , cl
        )
    }

    def insert[K, N, NN, VV]
        ( cf: SuperColumnFamily[K, N, NN, VV]
        )
        ( key:  K
        , path: N
        , col:  Column[NN, VV]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Unit] = {
        val SuperColumnFamily(_, name, keyCodec, nameCodec, colNameCodec, colValCodec) = cf
        Thrift.insert(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name, Some(path)))(nameCodec)
        , Column.convert(col)(colNameCodec, colValCodec)
        , cl
        )
    }

    def remove[K, N, V]
        ( cf: ColumnFamily[K, N, V]
        )
        ( key:  K
        , path: Option[N] // None to remove row
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Unit] = {
        val ColumnFamily(_, name, keyCodec, nameCodec, valCodec) = cf
        Thrift.remove(
          Key.convert(Key(key))(keyCodec)
        , ColumnPath.convert(ColumnPath(name, path, None))(nameCodec, null)
        , System.currentTimeMillis * 1000
        , cl
        )
    }

    def remove[K, N, NN, VV]
        ( cf: SuperColumnFamily[K, N, NN, VV]
        )
        ( key:  K
        , path: Option[N] // None to remove row
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Unit] = {
        val SuperColumnFamily(_, name, keyCodec, nameCodec, colNameCodec, _) = cf
        Thrift.remove(
          Key.convert(Key(key))(keyCodec)
        , ColumnPath.convert(ColumnPath(name, None, path))(colNameCodec, nameCodec)
        , System.currentTimeMillis * 1000
        , cl
        )
    }

    def remove[K, N]
        ( cf: CounterColumnFamily[K, N]
        )
        ( key:  K
        , path: Option[N] // None to remove row
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Unit] = {
        val CounterColumnFamily(_, name, keyCodec, nameCodec) = cf
        Thrift.removeCounter(
          Key.convert(Key(key))(keyCodec)
        , ColumnPath.convert(ColumnPath(name, path, None))(nameCodec, null)
        , cl
        )
    }

    def remove[K, N, NN]
        ( cf: SuperCounterColumnFamily[K, N, NN]
        )
        ( key:  K
        , path: Option[N] // None to remove row
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Unit] = {
        val SuperCounterColumnFamily(_, name, keyCodec, nameCodec, colNameCodec) = cf
        Thrift.removeCounter(
          Key.convert(Key(key))(keyCodec)
        , ColumnPath.convert(ColumnPath(name, None, path))(colNameCodec, nameCodec)
        , cl
        )
    }

    def getSlice[K, N, V]
        ( cf: ColumnFamily[K, N, V]
        )
        ( key:  K
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Seq[Column[N, V]]] = {
        val ColumnFamily(_, name, keyCodec, nameCodec, valCodec) = cf
        Thrift.getSlice(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        ).map(_.flatMap(Column.convertCosc(_)(nameCodec, valCodec)))
    }

    def getSlice[K, N, NN, VV]
        ( cf: SuperColumnFamily[K, N, NN, VV]
        )
        ( key:  K
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Seq[SuperColumn[N, NN, VV]]] = {
        val SuperColumnFamily(_, name, keyCodec, nameCodec, colNameCodec, colValCodec) = cf
        Thrift.getSlice(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        ).map(_.flatMap(SuperColumn.convertCosc(_)(nameCodec, colNameCodec, colValCodec)))
    }

    def getSlice[K, N]
        ( cf: CounterColumnFamily[K, N]
        )
        ( key:  K
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Seq[CounterColumn[N]]] = {
        val CounterColumnFamily(_, name, keyCodec, nameCodec) = cf
        Thrift.getSlice(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        ).map(_.flatMap(CounterColumn.convertCosc(_)(nameCodec)))
    }

    def getSlice[K, N, NN]
        ( cf: SuperCounterColumnFamily[K, N, NN]
        )
        ( key:  K
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Seq[CounterSuperColumn[N, NN]]] = {
        val SuperCounterColumnFamily(_, name, keyCodec, nameCodec, colNameCodec) = cf
        Thrift.getSlice(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        ).map(_.flatMap(CounterSuperColumn.convertCosc(_)(nameCodec, colNameCodec)))
    }

    def getCount[K, N, V]
        ( cf: ColumnFamily[K, N, V]
        )
        ( key:  K
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Int] = {
        val ColumnFamily(_, name, keyCodec, nameCodec, valCodec) = cf
        Thrift.getCount(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        )
    }

    def getCount[K, N, NN, VV]
        ( cf: SuperColumnFamily[K, N, NN, VV]
        )
        ( key:  K
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Int] = {
        val SuperColumnFamily(_, name, keyCodec, nameCodec, _, _) = cf
        Thrift.getCount(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        )
    }

    def getCount[K, N]
        ( cf: CounterColumnFamily[K, N]
        )
        ( key:  K
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Int] = {
        val CounterColumnFamily(_, name, keyCodec, nameCodec) = cf
        Thrift.getCount(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        )
    }

    def getCount[K, N, NN]
        ( cf: SuperCounterColumnFamily[K, N, NN]
        )
        ( key:  K
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Int] = {
        val SuperCounterColumnFamily(_, name, keyCodec, nameCodec, _) = cf
        Thrift.getCount(
          Key.convert(Key(key))(keyCodec)
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        )
    }

    def multigetSlice[K, N, V]
        ( cf: ColumnFamily[K, N, V]
        )
        ( keys: Seq[K]
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Map[K, Seq[Column[N, V]]]] = {
        val ColumnFamily(_, name, keyCodec, nameCodec, valCodec) = cf
        Thrift.multigetSlice(
          keys.map(key => Key.convert(Key(key))(keyCodec))
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        ).map(_.foldLeft(HashMap.empty[K, Seq[Column[N, V]]]) {
            case (h, (k, cols)) =>
                h += ( keyCodec.decode(k) ->
                       cols.flatMap(Column.convertCosc(_)(nameCodec, valCodec))
                     )
        }.toMap)
    }

    def multigetSlice[K, N, NN, VV]
        ( cf: SuperColumnFamily[K, N, NN, VV]
        )
        ( keys: Seq[K]
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Map[K, Seq[SuperColumn[N, NN, VV]]]] = {
        val SuperColumnFamily(_, name, keyCodec, nameCodec, colNameCodec, colValCodec) = cf
        Thrift.multigetSlice(
          keys.map(key => Key.convert(Key(key))(keyCodec))
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        ).map(_.foldLeft(HashMap.empty[K, Seq[SuperColumn[N, NN, VV]]]) {
            case (h, (k, cols)) =>
                h += ( keyCodec.decode(k) ->
                       cols.flatMap(
                          SuperColumn.convertCosc(_)( nameCodec
                                                    , colNameCodec
                                                    , colValCodec
                                                    )
                       )
                     )
        }.toMap)
    }

    def multigetSlice[K, N]
        ( cf: CounterColumnFamily[K, N]
        )
        ( keys: Seq[K]
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Map[K, Seq[CounterColumn[N]]]] = {
        val CounterColumnFamily(_, name, keyCodec, nameCodec) = cf
        Thrift.multigetSlice(
          keys.map(key => Key.convert(Key(key))(keyCodec))
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        ).map(_.foldLeft(HashMap.empty[K, Seq[CounterColumn[N]]]) {
            case (h, (k, cols)) =>
                h += ( keyCodec.decode(k) ->
                       cols.flatMap(CounterColumn.convertCosc(_)(nameCodec))
                     )
        }.toMap)
    }

    def multigetSlice[K, N, NN]
        ( cf: SuperCounterColumnFamily[K, N, NN]
        )
        ( keys: Seq[K]
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Map[K, Seq[CounterSuperColumn[N, NN]]]] = {
        val SuperCounterColumnFamily(_, name, keyCodec, nameCodec, colNameCodec) = cf
        Thrift.multigetSlice(
          keys.map(key => Key.convert(Key(key))(keyCodec))
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        ).map(_.foldLeft(HashMap.empty[K, Seq[CounterSuperColumn[N, NN]]]) {
            case (h, (k, cols)) =>
                h += ( keyCodec.decode(k) ->
                       cols.flatMap(
                         CounterSuperColumn.convertCosc(_)( nameCodec
                                                          , colNameCodec
                                                          )
                       )
                     )
        }.toMap)
    }

    def multigetCount[K, N, V]
        ( cf: ColumnFamily[K, N, V]
        )
        ( keys: Seq[K]
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Map[K, Int]] = {
        val ColumnFamily(_, name, keyCodec, nameCodec, _) = cf
        Thrift.multigetCount(
          keys.map(key => Key.convert(Key(key))(keyCodec))
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        ).map(_.foldLeft(HashMap.empty[K, Int]) {
            case (h, (k, n)) => h += keyCodec.decode(k) -> n
        }.toMap)
    }

    def multigetCount[K, N, NN, VV]
        ( cf: SuperColumnFamily[K, N, NN, VV]
        )
        ( keys: Seq[K]
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Map[K, Int]] = {
        val SuperColumnFamily(_, name, keyCodec, nameCodec, _, _) = cf
        Thrift.multigetCount(
          keys.map(key => Key.convert(Key(key))(keyCodec))
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        ).map(_.foldLeft(HashMap.empty[K, Int]) {
            case (h, (k, n)) => h += keyCodec.decode(k) -> n
        }.toMap)
    }

    def multigetCount[K, N]
        ( cf: CounterColumnFamily[K, N]
        )
        ( keys: Seq[K]
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Map[K, Int]] = {
        val CounterColumnFamily(_, name, keyCodec, nameCodec) = cf
        Thrift.multigetCount(
          keys.map(key => Key.convert(Key(key))(keyCodec))
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        ).map(_.foldLeft(HashMap.empty[K, Int]) {
            case (h, (k, n)) => h += keyCodec.decode(k) -> n
        }.toMap)
    }

    def multigetCount[K, N, NN]
        ( cf: SuperCounterColumnFamily[K, N, NN]
        )
        ( keys: Seq[K]
        , pred: SlicePredicate[N]
        , cl:   ConsistencyLevel
        )
    : Task[ThriftClient, Map[K, Int]] = {
        val SuperCounterColumnFamily(_, name, keyCodec, nameCodec, _) = cf
        Thrift.multigetCount(
          keys.map(key => Key.convert(Key(key))(keyCodec))
        , ColumnParent.convert(ColumnParent(name))(nameCodec)
        , SlicePredicate.convert(pred)(nameCodec)
        , cl
        ).map(_.foldLeft(HashMap.empty[K, Int]) {
            case (h, (k, n)) => h += keyCodec.decode(k) -> n
        }.toMap)
    }

    def batchMutate(bm: BatchMutation, cl: ConsistencyLevel)
    : Task[ThriftClient, Unit] =
        Thrift.batchMutate(BatchMutation.freeze(bm), cl)


    object Thrift {

        import scala.collection.JavaConversions._

        import org.apache.cassandra.thrift
        import org.apache.thrift.async._

        import bob.Bob._
        import Internal._


        def login(username: String, password: String)
        : Task[ThriftClient, Unit] =
            mkTask(_.login(AuthReq(username, password), _))

        def setKeyspace(name: String): Task[ThriftClient, Unit] =
            mkTask(_.set_keyspace(name, _))

        def get
            ( key: ByteBuffer
            , cp:  thrift.ColumnPath
            , cl:  thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, thrift.ColumnOrSuperColumn] =
            mkTask(_.get(key, cp, cl, _))

        def getSlice
            ( key: ByteBuffer
            , cp:  thrift.ColumnParent
            , sp:  thrift.SlicePredicate
            , cl:  thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Seq[thrift.ColumnOrSuperColumn]] =
            mkTask(_.get_slice(key, cp, sp, cl, _))

        def getCount
            ( key: ByteBuffer
            , cp:  thrift.ColumnParent
            , sp:  thrift.SlicePredicate
            , cl:  thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Int] =
            mkTask(_.get_count(key, cp, sp, cl, _))

        def multigetSlice
            ( keys: Seq[ByteBuffer]
            , cp:   thrift.ColumnParent
            , sp:   thrift.SlicePredicate
            , cl:   thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Map[ByteBuffer,Seq[thrift.ColumnOrSuperColumn]]] =
            mkTask(_.multiget_slice(keys, cp, sp, cl, _))

        def multigetCount
            ( keys: Seq[ByteBuffer]
            , cp:   thrift.ColumnParent
            , sp:   thrift.SlicePredicate
            , cl:   thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Map[ByteBuffer,Int]] =
            mkTask(_.multiget_count(keys, cp, sp, cl, _))

        def getRangeSlices
            ( cp: thrift.ColumnParent
            , sp: thrift.SlicePredicate
            , kr: thrift.KeyRange
            , cl: thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Seq[thrift.KeySlice]] =
            mkTask(_.get_range_slices(cp, sp, kr, cl, _))

        def getPagedSlice
            ( cf:       String
            , kr:       thrift.KeyRange
            , startCol: ByteBuffer
            , cl:       thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Seq[thrift.KeySlice]] =
            mkTask(_.get_paged_slice(cf, kr, startCol, cl, _))

        def insert
            ( key: ByteBuffer
            , cp:  thrift.ColumnParent
            , col: thrift.Column
            , cl:  thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Unit] =
            mkTask(_.insert(key, cp, col, cl, _))

        def add
            ( key: ByteBuffer
            , cp:  thrift.ColumnParent
            , col: thrift.CounterColumn
            , cl:  thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Unit] =
            mkTask(_.add(key, cp, col, cl, _))

        def remove
            ( key: ByteBuffer
            , cp:  thrift.ColumnPath
            , ts:  Long
            , cl:  thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Unit] =
            mkTask(_.remove(key, cp, ts, cl, _))

        def removeCounter
            ( key: ByteBuffer
            , cp:  thrift.ColumnPath
            , cl:  thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Unit] =
            mkTask(_.remove_counter(key, cp, cl, _))

        def batchMutate
            ( mutations: Map[ByteBuffer,Map[String,Seq[thrift.Mutation]]]
            , cl:        thrift.ConsistencyLevel = thrift.ConsistencyLevel.ONE
            )
        : Task[ThriftClient, Unit] =
            mkTask(_.batch_mutate(mutationMapToJava(mutations), cl, _))

        def truncate(cf: String): Task[ThriftClient, Unit] =
            mkTask(_.truncate(cf, _))


        // "Meta-APIs"

        def describeSchemaVersions()
        : Task[ThriftClient, Map[String,Seq[String]]] =
            mkTask(_.describe_schema_versions(_))

        def describeKeyspaces(): Task[ThriftClient, Seq[thrift.KsDef]] =
            mkTask(_.describe_keyspaces(_))

        def describeClusterName(): Task[ThriftClient, String] =
            mkTask(_.describe_cluster_name(_))

        def describeVersion(): Task[ThriftClient, String] =
            mkTask(_.describe_version(_))

        def describeRing(keyspace: String)
        : Task[ThriftClient, Seq[thrift.TokenRange]] =
            mkTask(_.describe_ring(keyspace, _))

        def describePartitioner(): Task[ThriftClient, String] =
            mkTask(_.describe_partitioner(_))

        def describeSnitch(): Task[ThriftClient, String] =
            mkTask(_.describe_snitch(_))

        def describeKeyspace(keyspace: String)
        : Task[ThriftClient, thrift.KsDef] =
            mkTask(_.describe_keyspace(keyspace, _))

        // experimental  API for hadoop/parallel query support
        def describeSplits
            ( cf:           String
            , startToken:   String
            , endToken:     String
            , keysPerSplit: Int
            )
        : Task[ThriftClient, List[String]] =
            mkTask(_.describe_splits(cf, startToken, endToken, keysPerSplit, _))

        // "system" operations

        type SchemaId = String

        def systemAddColumnFamily(cfdef: thrift.CfDef)
        : Task[ThriftClient, SchemaId] =
            mkTask(_.system_add_column_family(cfdef, _))

        def systemDropColumnFamily(cf: String)
        : Task[ThriftClient, SchemaId] =
            mkTask(_.system_drop_column_family(cf, _))

        def systemAddKeyspace(ksdef: thrift.KsDef)
        : Task[ThriftClient, SchemaId] =
            mkTask(_.system_add_keyspace(ksdef, _))

        def systemDropKeyspace(ks: String)
        : Task[ThriftClient, SchemaId] =
            mkTask(_.system_drop_keyspace(ks, _))

        def systemUpdateKeyspace(ksdef: thrift.KsDef)
        : Task[ThriftClient, SchemaId] =
            mkTask(_.system_update_keyspace(ksdef, _))

        def systemUpdateColumnFamily(cfdef: thrift.CfDef)
        : Task[ThriftClient, SchemaId] =
            mkTask(_.system_update_column_family(cfdef, _))


        // CQL

        def executeCqlQuery(query: ByteBuffer, comp: thrift.Compression)
        : Task[ThriftClient, thrift.CqlResult] =
            mkTask(_.execute_cql_query(query, comp, _))

        def prepareCqlQuery(query: ByteBuffer, comp: thrift.Compression)
        : Task[ThriftClient, thrift.CqlPreparedResult] =
            mkTask(_.prepare_cql_query(query, comp, _))

        def executePreparedCqlQuery(id: Int, values: Seq[ByteBuffer])
        : Task[ThriftClient, thrift.CqlResult] =
            mkTask(_.execute_prepared_cql_query(id, values, _))

        def setCqlVersion(v: String): Task[ThriftClient, Unit] =
            mkTask(_.set_cql_version(v, _))
    }

    private[this]
    object Internal {

        import java.nio.ByteBuffer

        import scala.concurrent.SyncVar
        import scala.collection.JavaConversions._
        import scala.collection.JavaConverters._

        import org.apache.cassandra.{ thrift => cass }
        import cass.{ AuthenticationRequest => CassAuthReq
                    , Mutation => CassMutation
                    }
        import cass.Cassandra.AsyncClient
        import cass.Cassandra.AsyncClient._
        import org.apache.thrift.async._

        import bob.Util._


        type TracedResult[A] = (SyncVar[Result[A]], Timestamp)

        implicit
        def mkTask[A](f: (AsyncClient, TracedResult[A]) => Unit)
        : Task[ThriftClient, A] =
            task { c =>
                val res = new SyncVar[Result[A]]
                c.runWith(f(_, new TracedResult(res, now())))
                promise(res.get)
            }

        implicit
        def callback[A, B](p: TracedResult[B])
        : AsyncMethodCallback[A] =
            new AsyncMethodCallback[A] {
                def put(r: Either[Throwable, B]) =
                    p._1 put Result(r, Latency(p._2))

                def onComplete(r: A) {
                  try { r match {
                      // damn you, thrift authors: `getResult` is part of the
                      // interface of `TAsyncMethodCall`, but not declared in
                      // the base class, so generated methods (which exist even
                      // for `void` results) don't inherit
                      case x: login_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: get_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: set_keyspace_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: get_slice_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: get_count_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: multiget_slice_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: multiget_count_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: get_range_slices_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: get_paged_slice_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: insert_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: add_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: remove_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: remove_counter_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: batch_mutate_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: truncate_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: describe_schema_versions_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: describe_keyspaces_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: describe_cluster_name_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: describe_version_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: describe_partitioner_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: describe_snitch_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: describe_keyspace_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: describe_splits_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: system_add_column_family_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: system_drop_column_family_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: system_add_keyspace_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: system_drop_keyspace_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: system_update_keyspace_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: system_update_column_family_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: execute_cql_query_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: prepare_cql_query_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: execute_prepared_cql_query_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x: set_cql_version_call =>
                          put (Right(x.getResult.asInstanceOf[B]))
                      case x => sys.error("wtf?: " + x)
                  }}
                  catch { case e => put (Left(e)) }
                }
                def onError(e: Exception) { put (Left(e)) }
            }


        // oh my
        import java.util.{ Map => JMap, List => JList }
        implicit
        def mutationMapToJava
            ( mm: Map[ByteBuffer,Map[String,Seq[CassMutation]]]
            )
        : JMap[ByteBuffer,JMap[String,JList[CassMutation]]] =
            mm.mapValues(_.mapValues(_.asJava).asJava).asJava

        implicit
        def AuthReq(username: String, password: String): CassAuthReq =
            new CassAuthReq(Map(username -> password))
    }
}


// vim: set ts=4 sw=4 et:
