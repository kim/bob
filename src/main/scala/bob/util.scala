package bob

object Util {

    import java.util.concurrent.{ Callable, ExecutorService, Future, TimeUnit }


    type Host = (String, Int)


    case class Timestamp(value: Long)
    object Timestamp {
        def apply(): Timestamp = Timestamp(System.currentTimeMillis)
        def now = apply
    }

    def now(): Timestamp = Timestamp.now

    case class Latency(l: Long, t: Timestamp) {
        def +(othr: Latency): Latency = copy(l + othr.l, now())
    }
    object Latency {
        def apply(): Latency = Latency(0, now())

        def apply(old: Latency): Latency = {
            val t = now()
            old.copy(old.l + (t.value - old.t.value), t)
        }

        def apply(last: Timestamp): Latency = {
            val t = now()
            Latency(t.value - last.value, t)
        }
    }

    case class Duration(value: Long, unit: TimeUnit) {
        def days:         Long = unit toDays    value
        def hours:        Long = unit toHours   value
        def microseconds: Long = unit toMicros  value
        def milliseconds: Long = unit toMillis  value
        def minutes:      Long = unit toMinutes value
        def nanoseconds:  Long = unit toNanos   value
        def seconds:      Long = unit toSeconds value
    }

    final class LongDuration(value: Long) {
        import TimeUnit._

        def days:         Duration = duration(DAYS)
        def hours:        Duration = duration(HOURS)
        def microseconds: Duration = duration(MICROSECONDS)
        def milliseconds: Duration = duration(MILLISECONDS)
        def minutes:      Duration = duration(MINUTES)
        def nanoseconds:  Duration = duration(NANOSECONDS)
        def seconds:      Duration = duration(SECONDS)

        private[this]
        def duration(unit: TimeUnit): Duration = Duration(value, unit)
    }

    implicit def longToDuration(l: Long): LongDuration = new LongDuration(l)


    trait ThreadPool[F[_]] {
        def submit[A](c: Callable[A]): F[A]
        def shutdown()
    }

    object ThreadPool {
        implicit
        def JavaThreadPool(x: ExecutorService): ThreadPool[Future]
        = new ThreadPool[Future] {
            def submit[A](c: Callable[A]) = x submit c
            def shutdown() = x.shutdown()
        }
    }

    implicit
    def callable[A](f: => A): Callable[A] = new Callable[A] { def call = f }
}


// vim: set ts=4 sw=4 et:
