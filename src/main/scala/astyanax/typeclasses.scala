package astyanax

trait Typeclasses {

    import java.util.concurrent.{ TimeUnit, TimeoutException }

    import scala.concurrent.SyncVar

    import org.apache.cassandra.thrift.Cassandra


    trait Client {
        val thrift: Cassandra.AsyncClient
        def close()
    }

    // basically an Either monad, encapsulates the response (`A`), or any errors
    case class Result[A](value: Either[Throwable, A]) {
        def map[B](f: A => B): Result[B] =
            flatMap(x => Right(f(x)))

        def flatMap[B](f: A => Result[B]): Result[B] =
            value match {
                case Left(x)  => Left(x)
                case Right(x) => f(x)
            }
    }

    implicit
    def eitherToResult[A](e: Either[Throwable, A]): Result[A] = Result(e)

    implicit
    def resultToEither[A](r: Result[A]): Either[Throwable, A] = r.value

    // a monad of cassandra API calls. can be used to sequence calls, necessary
    // if those need to use the same connection (such as `setKeyspace` followed
    // by `get`). the computation stops on the first error.
    trait Task[A] {
        def apply(c: Client): Promise[A]

        def map[B](f: A => B): Task[B] =
            flatMap(x => task(c => promise(c -> Result(Right(f(x))))))

        def flatMap[B](f: A => Task[B]): Task[B] =
            task { c => apply(c).flatMap { r => promise { s => s -> r.flatMap { a =>
                f(a)(c).eval(s)
            }}}}
    }

    def task[A](f: Client => Promise[A]): Task[A] =
        new Task[A] { def apply(c: Client) = f(c) }

    object Task {
        def lift[A](f: => A): Task[A] =
            task(c => promise(c -> Result(Right(f))))

        def barrier
            ( timeout: (Long, TimeUnit)
            , t1:      Long = System.currentTimeMillis
            )
        : Task[Unit] =
            task { c =>
                val t2 = System.currentTimeMillis
                val r  = if (t2 - t1 > timeout._2.toMillis(timeout._1))
                             Left(new TimeoutException())
                         else
                             Right(())

                promise(c -> Result(r))
            }
    }

    // since we're wrapping the async API, when running a `Task`, we'll get back
    // a promise, which will eventually yield the result. notice that this is
    // just a state monad, threading the client
    type Promise[A] = State[Client, Result[A]]

    def promise[A](f: (Client, SyncVar[Result[A]]) => Unit)
                  (c: Client)
    : Promise[A] = {
        val res = new SyncVar[Result[A]]
        f(c, res)
        promise(c -> res.get)
    }

    def promise[A](g: => (Client, Result[A])): Promise[A] =
        state(s => g)

    final def promise[A](f: Client => (Client, Result[A])): Promise[A] =
        state(s => f(s))


    trait State[S, +A] {
        def apply(s: S): (S, A)

        def eval(s: S): A = apply(s)._2

        def map[B](f: A => B): State[S, B] =
            state(apply(_) match { case (s,a) => s -> f(a) })

        def flatMap[B](f: A => State[S, B]): State[S, B] =
            state(apply(_) match { case (s,a) => f(a)(s) })
    }

    def state[S, A](f: S => (S, A)): State[S, A] = new State[S, A] {
        def apply(s: S) = f(s)
    }
}

object Typeclasses extends Typeclasses


// vim: set ts=4 sw=4 et:
