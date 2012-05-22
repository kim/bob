package object astyanax {
    object Astyanax
        extends Types
        with Api
        with Conversions
        with IO

    import Types._
    // perform an arbitrary side-effect as a `Task`
    implicit def effect[A](f: => A): Task[A] =
        task(c => promise(Result(Right(f)) -> c))
}


// vim: set ts=4 sw=4 et:
