package bob.pool

object ResourcePool {

    import java.util.{ Date, Timer, TimerTask }
    import java.util.concurrent.{ BlockingDeque, LinkedBlockingDeque }
    import java.util.concurrent.atomic.AtomicInteger

    import scala.collection.JavaConversions._

    import bob.Util._


    type CreateResource[A]  = Unit => A
    type DestroyResource[A] = A => Unit

    final case class Entry[A]
        ( entry:   A
        , lastUse: Date
        )

    final case class LocalPool[A]
        ( entries: BlockingDeque[Entry[A]]
        , inUse:   AtomicInteger
        )

    sealed case class Pool[A]
        ( create:       CreateResource[A]
        , destroy:      DestroyResource[A]
        , numStripes:   Int
        , idleTime:     Duration
        , maxResources: Int
        , localPools:   IndexedSeq[LocalPool[A]]
        )


    def createPool[A]
        ( create:       CreateResource[A]
        , destroy:      DestroyResource[A]
        , numStripes:   Int
        , idleTime:     Duration
        , maxResources: Int
        )
    : Pool[A] = {
        if (numStripes < 1)
            sys.error("invalid stripe count " + numStripes)
        if (maxResources < 1)
            sys.error("invalid maximum resource count " + maxResources)
        if (idleTime.milliseconds < 500)
            sys.error("invalid idle time " + idleTime)

        val pools = (0 until numStripes) map { _ =>
          LocalPool( new LinkedBlockingDeque[Entry[A]]()
                   , new AtomicInteger(0)
                   )
        }
        val reaper = {
          val t = new Timer(true)
          t.schedule( new TimerTask { def run() = reap(destroy, idleTime, pools) }
                    , idleTime.milliseconds
                    , idleTime.milliseconds
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
        } catch { case e => destroyResource(pool, local, res); throw e }
    }

    def takeResource[A](pool: Pool[A]): (A, LocalPool[A]) = {
        val local = pool.localPools(
            Thread.currentThread.getId.hashCode % pool.numStripes)
        val entry = Option(local.entries.poll()) orElse {
                        if (local.inUse.get >= pool.maxResources)
                            Some(local.entries.take())
                        else {
                            local.inUse.incrementAndGet()
                            try   { Some(Entry(pool.create(), new Date)) }
                            catch { case e => local.inUse.decrementAndGet(); throw e }
                        }
                    } map (_ entry)

        entry.get -> local
    }

    def tryWithResource[A, B](pool: Pool[A])(act: A => B): Option[B] =
        tryTakeResource(pool) match {
            case None => None
            case Some((res, local)) =>
                try {
                    val ret = act(res)
                    putResource(local, res)
                    Some(ret)
                } catch { case e => destroyResource(pool, local, res); throw e }
        }

    def tryTakeResource[A](pool: Pool[A]): Option[(A, LocalPool[A])] = {
        val local = pool.localPools(
            Thread.currentThread.getId.hashCode % pool.numStripes)
        val entry = Option(local.entries.poll()) orElse {
                        if (local.inUse.get >= pool.maxResources)
                            None
                        else {
                            local.inUse.incrementAndGet()
                            try   { Some(Entry(pool.create(), new Date)) }
                            catch { case e => local.inUse.decrementAndGet(); throw e }
                        }
                    } map (_ entry)

        entry map (_ -> local)
    }


    def putResource[A](localPool: LocalPool[A], r: A): Unit =
        localPool.entries.push(Entry(r, new Date))

    def destroyResource[A](pool: Pool[A], localPool: LocalPool[A], r: A) {
        localPool.inUse.decrementAndGet()
        localPool.entries.remove(r)
        pool.destroy(r)
    }

    def destroyAll[A](pool: Pool[A]) {
        pool.localPools.flatMap { local =>
            val e = local.entries.iterator.toIterable
            local.inUse.addAndGet(-e.size)
            local.entries.removeAll(e)
            e.map(_ entry)
        }.foreach(e => try { pool.destroy(e) } catch { case _ => () })
    }

    def numAllocated[A](pool: Pool[A]): Int =
        pool.localPools.map(_.inUse.get).sum

    def numActive[A](pool: Pool[A]): Int =
        pool.localPools.map(_.entries.size).sum

    def remainingCapacity[A](pool: Pool[A]): Int =
        (pool.maxResources * pool.localPools.size) - numAllocated(pool)

    def exhausted[A](pool: Pool[A]): Boolean =
        (pool.maxResources * pool.localPools.size) - numActive(pool) == 0

    private[this]
    def reap[A]
      ( destroy:  DestroyResource[A]
      , idleTime: Duration
      , pools:    IndexedSeq[LocalPool[A]]
      )
    : Unit = {
        val now = System.currentTimeMillis
        def isStale(e: Entry[A]): Boolean =
            now - e.lastUse.getTime > idleTime.milliseconds

        pools.flatMap { pool =>
            val (stale, fresh) = pool.entries.partition(isStale)
            if (stale.size > 0) {
                pool.inUse.addAndGet(-stale.size)
                pool.entries.retainAll(fresh)
            }
            stale.map(_ entry)
        }.foreach(e => try { destroy(e) } catch { case _ => () })
    }
}


// vim: set ts=4 sw=4 et:
