package bob

object Util {

    import java.util.concurrent.{ Callable, ExecutorService, Future, TimeUnit }


    case class Timestamp(value: Long)
    object Timestamp {
        def apply(): Timestamp = Timestamp(System.currentTimeMillis)
        def now = apply
    }

    def now(): Timestamp = Timestamp.now

    case class Latency(l: Long, t: Timestamp)
    object Latency {
        def apply(): Latency =
            Latency(0, now())

        def apply(old: Latency): Latency = {
            val t = now()
            old.copy(l = old.l + (t.value - old.t.value), t = t)
        }

        def apply(last: Timestamp): Latency = {
            val t = now()
            Latency(t.value - last.value, t)
        }
    }


    trait ThreadPool[F[_]] {
        def submit[A](c: Callable[A]): F[A]
        def shutdown()
    }

    object ThreadPool {
        implicit def JavaThreadPool(x: ExecutorService)
        : ThreadPool[Future] = new ThreadPool[Future] {
            def submit[A](c: Callable[A]) = x submit c
            def shutdown() = x.shutdown()
        }
    }

    def callable[A](f: => A): Callable[A] = new Callable[A] { def call = f }
}


// vim: set ts=4 sw=4 et:
