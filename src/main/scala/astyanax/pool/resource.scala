package astyanax.pool

object ResourcePool {

    import java.util.{ Date, Timer, TimerTask }
    import java.util.concurrent.{ ArrayBlockingQueue, BlockingQueue, TimeUnit }
    import java.util.concurrent.atomic.AtomicInteger

    import scala.collection.JavaConversions._


    type CreateResource[A]  = Unit => A
    type DestroyResource[A] = A => Unit

    final case class Entry[A]
        ( entry:   A
        , lastUse: Date
        )

    final case class LocalPool[A]
        ( entries: BlockingQueue[Entry[A]]
        , inUse:   AtomicInteger
        )

    sealed case class Pool[A]
        ( create:       CreateResource[A]
        , destroy:      DestroyResource[A]
        , numStripes:   Int
        , idleTime:     (Long, TimeUnit)
        , maxResources: Int
        , localPools:   IndexedSeq[LocalPool[A]]
        )


    def createPool[A]
        ( create:       CreateResource[A]
        , destroy:      DestroyResource[A]
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
          LocalPool(
            new ArrayBlockingQueue[Entry[A]](maxResources)
          , new AtomicInteger(0)
          )
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

    def putResource[A](localPool: LocalPool[A], r: A): Unit =
        localPool.entries.offer(Entry(r, new Date))

    def destroyResource[A](pool: Pool[A], localPool: LocalPool[A], r: A) {
        localPool.inUse.set(0)
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

    private[this]
    def reap[A]
      ( destroy:  DestroyResource[A]
      , idleTime: (Long, TimeUnit)
      , pools:    IndexedSeq[LocalPool[A]]
      )
    : Unit = {
        val now  = System.currentTimeMillis
        val idle = toMillis(idleTime)
        def isStale(e: Entry[A]): Boolean = now - e.lastUse.getTime > idle

        pools.flatMap { pool =>
            val (stale, fresh) = pool.entries.partition(isStale)
            if (stale.size > 0) {
                pool.inUse.addAndGet(-stale.size)
                pool.entries.retainAll(fresh)
            }
            stale.map(_ entry)
        }.foreach(e => try { destroy(e) } catch { case _ => () })
    }

    private[this]
    def toMillis(idleTime: (Long, TimeUnit)): Long =
        idleTime match { case (i, unit) => unit.toMillis(i) }
}


// vim: set ts=4 sw=4 et:
