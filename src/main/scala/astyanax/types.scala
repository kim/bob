package astyanax

trait Types {

    import scala.concurrent.SyncVar

    import org.apache.cassandra.thrift._
    import org.apache.thrift.transport._


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
            flatMap(x => task(c => promise(Result(Right(f(x))) -> c)))

        def flatMap[B](f: A => Task[B]): Task[B] =
            task(c => apply(c).flatMap(a => f(a).apply(c)))
    }

    def task[A](f: Client => Promise[A]): Task[A] =
        new Task[A] { def apply(c: Client) = f(c) }

    // since we're wrapping the async API, when running a `Task`, we'll get back
    // a promise, which will eventually yield the result. note that is threads
    // the `Client` as well, so `Promises` can be sequenced in the `Task` monad
    trait Promise[A] {
        def get: (Result[A], Client)

        def map[B](f: A => B): Promise[B] =
            promise(get match { case (a,c) => a.map(f) -> c })

        def flatMap[B](f: A => Promise[B]): Promise[B] =
            get match { case (a,c) => promise(a.flatMap(x => f(x).get._1) -> c) }
    }

    def promise[A](f: (Client, SyncVar[Result[A]]) => Unit)
                  (c: Client)
    : Promise[A] = {
        val res = new SyncVar[Result[A]]
        f(c, res)
        promise(res.get -> c)
    }

    def promise[A](g: => (Result[A], Client)): Promise[A] =
        new Promise[A] { def get = g }


    // a handy reader monad
    trait Reader[A] {
        def apply[B](b: B): A

        def map[B](f: A => B): Reader[B] = new Reader[B] {
            def apply[C](x: C) = f(Reader.this.apply(x))
        }

        def flatMap[B](f: A => Reader[B]): Reader[B] = new Reader[B] {
            def apply[C](x: C) = f(Reader.this.apply(x))(x)
        }
    }

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

object Types extends Types


// vim: set ts=4 sw=4 et:
