package astyanax

object ResourcePool {

    import java.util.{ Date
                     , Timer
                     , TimerTask
                     }
    import java.util.concurrent.{ ArrayBlockingQueue
                                , BlockingQueue
                                , TimeUnit
                                }
    import java.util.concurrent.atomic.AtomicInteger

    import scala.collection.JavaConversions._


    final case class Entry[A]
        ( entry:   A
        , lastUse: Date
        )

    final case class LocalPool[A]
        ( entries: BlockingQueue[Entry[A]]
        )

    sealed case class Pool[A]
        ( create:       Function1[Unit, A]
        , destroy:      Function1[A, Unit]
        , numStripes:   Int
        , idleTime:     (Long, TimeUnit)
        , maxResources: Int
        , localPools:   IndexedSeq[LocalPool[A]]
        )

    def createPool[A]
        ( create:       Function1[Unit, A]
        , destroy:      Function1[A, Unit]
        , numStripes:   Int
        , idleTime:     (Long, TimeUnit)
        , maxResources: Int
        )
    : Pool[A] = {
        if (numStripes < 1)
            sys.error("invalid stripe count " + numStripes)
        if (maxResources < 1)
            sys.error("invalid maximum resource count " + maxResources)

        val idle = toMillis(idleTime)
        if (idle < 500)
            sys.error("invalid idle time " + idleTime)

        val pools = (0 until numStripes) map { _ =>
          LocalPool(new ArrayBlockingQueue[Entry[A]](maxResources))
        }
        val reaper = {
          val t = new Timer(true)
          t.schedule(
            new TimerTask { def run() = reap(destroy, idleTime, pools) }
          , idle
          , idle
          )
          t
        }

        new Pool(create, destroy, numStripes, idleTime, maxResources, pools) {
            override def finalize() = reaper.cancel()
        }
    }

    def withResource[A, B](pool: Pool[A])(act: A => B): B = {
        val (res, local) = takeResource(pool)
        try {
            val ret = act(res)
            putResource(local, res)
            ret
        } catch {
          case e => destroyResource(pool, local, res); throw e
        }
    }

    def takeResource[A](pool: Pool[A]): (A, LocalPool[A]) = {
        val local = pool.localPools(
            Thread.currentThread.getId.hashCode % pool.numStripes)
        val entry = Option(local.entries.poll()).map(_ entry)
                        .getOrElse(pool.create())
        entry -> local
    }

    def putResource[A](localPool: LocalPool[A], r: A): Unit =
        localPool.entries.offer(Entry(r, new Date))

    def destroyResource[A](pool: Pool[A], localPool: LocalPool[A], r: A) {
        localPool.entries.remove(r)
        pool.destroy(r)
    }

    private[this] def reap[A]
      ( destroy:  Function1[A, Unit]
      , idleTime: (Long, TimeUnit)
      , pools:    IndexedSeq[LocalPool[A]]
      )
    : Unit = {
        val now  = System.currentTimeMillis
        val idle = toMillis(idleTime)
        def isStale(e: Entry[A]): Boolean = now - e.lastUse.getTime > idle

        pools.flatMap { pool =>
            val (stale, fresh) = pool.entries.partition(isStale)
            if (stale.size > 0)
                pool.entries.retainAll(fresh)
            stale.map(_ entry)
        }.foreach(e => try { destroy(e) } catch { case _ => () })
    }

    private[this] def toMillis(idleTime: (Long, TimeUnit)): Long =
      idleTime match { case (i, unit) => unit.toMillis(i) }
}


// vim: set ts=4 sw=4 et:
