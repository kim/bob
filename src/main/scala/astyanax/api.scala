package astyanax

trait Api {

    import java.nio.ByteBuffer

    import scala.concurrent.SyncVar

    import org.apache.cassandra.thrift._
    import org.apache.thrift.async._

    import Types._
    import Implicits._


    type SetKeyspace = Cassandra.AsyncClient.set_keyspace_call
    type Get = Cassandra.AsyncClient.get_call

    def setKeyspace(name: String): Task[Unit] =
        task(promise(_._1.set_keyspace(name, _)))

    def get(key: String, cf: String, col: String, cl: ConsistencyLevel)
    : Task[ColumnOrSuperColumn] =
        task(promise(
            _._1.get( ByteBuffer.wrap(key.getBytes)
                    , new ColumnPath(cf).setColumn(col.getBytes)
                    , cl
                    , _
                    )
        ))

    object Implicits {
        implicit
        def callback[A, B](p: SyncVar[Result[B]]): AsyncMethodCallback[A] =
            new AsyncMethodCallback[A] {
                def onComplete(r: A) {
                  try { r match {
                      // damn you, thift authors: `getResult` is part of the
                      // interface of `TAsyncMethodCall`, but not declared in
                      // the base class, so generated methods (which exist even
                      // for `void` results) don't inherit
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
    }
}

object Api extends Api


// vim: set ts=4 sw=4 et:
