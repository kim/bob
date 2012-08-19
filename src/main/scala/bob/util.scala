package bob

object Util {

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
}


// vim: set ts=4 sw=4 et:
