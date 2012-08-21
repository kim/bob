package bob

trait Typeclasses {

    import java.util.concurrent.{ TimeUnit, TimeoutException }
    import Util._


    trait Functor[F[_]] {
        def fmap[A, B](r: F[A])(f: A => B): F[B]
    }

    trait Bind[Z[_]] {
        def bind[A, B](a: Z[A])(f: A => Z[B]): Z[B]
    }

    trait MA[M[_], A] {
        val value: M[A]

        def map[B](f: A => B)(implicit t: Functor[M]): M[B] =
            t.fmap(value)(f)

        def >>=[B](f: A => M[B])(implicit b: Bind[M]): M[B] =
            b.bind(value)(f)
    }

    implicit
    def ma[M[_], A](a: M[A]): MA[M, A] = new MA[M, A] {
        val value = a
    }

    // basically an Either monad, encapsulates the response (`A`), or any errors
    case class Result[A](value: Either[Throwable, A], latency: Latency) {
        def map[B](f: A => B): Result[B] =
            flatMap(x => Right(f(x)))

        def flatMap[B](f: A => Result[B]): Result[B] =
            value match {
                case Left(x)  => Result(Left(x), Latency(latency))
                case Right(x) => val y = f(x); y.copy(latency = latency + y.latency)
            }
    }

    implicit
    def eitherToResult[A](e: Either[Throwable, A]): Result[A] =
        Result(e, Latency())

    implicit
    def resultToEither[A](r: Result[A]): Either[Throwable, A] = r.value

    implicit
    def ResultFunctor: Functor[Result] = new Functor[Result] {
        def fmap[A, B](r: Result[A])(f: A => B) = r map f
    }

    implicit
    def ResultBind: Bind[Result] = new Bind[Result] {
        def bind[A, B](r: Result[A])(f: A => Result[B]) = r flatMap f
    }

    // a monad of cassandra API calls. can be used to sequence calls, necessary
    // if those need to use the same connection (such as `setKeyspace` followed
    // by `get`). the computation stops on the first error.
    type Task[S, A] = StateT[({type λ[α]=Promise[S, α]})#λ, S, A]

    def task[S, A](f: S => Promise[S, A]): Task[S, A] =
        stateT[({type λ[α]=Promise[S, α]})#λ, S, A](c => promise(f(c)(c)))

    def task[S, A](f: => A): Task[S, A] =
        task(_ => promise(Result(Right(f), Latency())))

    def barrier[S]
        ( timeout: (Long, TimeUnit)
        , t1:      Long = System.currentTimeMillis
        )
    : Task[S, Unit] =
        task { c =>
            val t2 = System.currentTimeMillis
            val r  = if (t2 - t1 > timeout._2.toMillis(timeout._1))
                         Left(new TimeoutException())
                     else
                         Right(())

            promise(Result(r, Latency(t2 - t1, Timestamp(t2))))
        }

    // since we're wrapping the async API, when running a `Task`, we'll get back
    // a promise, which will eventually yield the result. notice that this is
    // just a state monad, threading the client
    type Promise[S, A] = StateT[Result, S, A]

    def promise[S, A](g: => Result[A]): Promise[S, A] =
        stateT(s => g map (a => s -> a))

    implicit
    def PromiseFunctor[S]: Functor[({type λ[α]=Promise[S, α]})#λ] =
        new Functor[({type λ[α]=Promise[S, α]})#λ] {
            def fmap[A, B](r: Promise[S, A])(f: A => B) = r map f
        }

    implicit
    def PromiseBind[S]: Bind[({type λ[α]=Promise[S, α]})#λ] =
        new Bind[({type λ[α]=Promise[S, α]})#λ] {
            def bind[A, B](r: Promise[S, A])(f: A => Promise[S, B]) = r flatMap f
        }

    implicit
    def PromiseMA[S, A](p: Promise[S, A]): MA[({type λ[α]=Promise[S, α]})#λ, A] =
        ma[({type λ[α]=Promise[S, α]})#λ, A](p)

    trait State[S, +A] {
        def apply(s: S): (S, A)

        def eval(s: S): A = apply(s)._2

        def map[B](f: A => B): State[S, B] =
            state(apply(_) match { case (s,a) => s -> f(a) })

        def flatMap[B](f: A => State[S, B]): State[S, B] =
            state(apply(_) match { case (s,a) => f(a)(s) })
    }

    def state[S, A](f: S => (S, A)): State[S, A] =
        new State[S, A] { def apply(s: S) = f(s) }

    implicit
    def StateFunctor[S]: Functor[({type λ[α]=State[S, α]})#λ] =
        new Functor[({type λ[α]=State[S, α]})#λ] {
            def fmap[A, B](r: State[S, A])(f: A => B) = r map f
        }

    implicit
    def StateBind[S]: Bind[({type λ[α]=State[S, α]})#λ] =
        new Bind[({type λ[α]=State[S, α]})#λ] {
            def bind[A, B](r: State[S, A])(f: A => State[S, B]) = r flatMap f
        }

    implicit
    def StateMA[S, A](s: State[S, A]): MA[({type λ[α]=State[S, α]})#λ, A] =
        ma[({type λ[α]=State[S, α]})#λ, A](s)

    trait StateT[M[_], S, A] {
        def apply(s: S): M[(S, A)]

        def eval(s: S)(implicit m: Functor[M]): M[A] =
            apply(s) map (_._2)

        def map[B](f: A => B)(implicit m: Functor[M]): StateT[M, S, B] =
            stateT(s => apply(s) map { case (s1,a) => s1 -> f(a) })

        def flatMap[B](f: A => StateT[M, S, B])(implicit m: Bind[M]): StateT[M, S, B] =
            stateT(apply(_) >>= (_ match { case (s1,a) => f(a)(s1) }))
    }

    def stateT[M[_], S, A](f: S => M[(S, A)]): StateT[M, S, A] =
        new StateT[M, S, A] { def apply(s: S) = f(s) }
}

object Typeclasses extends Typeclasses


// vim: set ts=4 sw=4 et:
