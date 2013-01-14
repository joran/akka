/**
 *  Copyright (C) 2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.util

import akka.util.Unsafe.{ instance ⇒ unsafe }
import java.util.concurrent.atomic.AtomicReferenceArray
import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, blocking }
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.Promise
import scala.concurrent.Future
import akka.actor.Cancellable
import scala.collection.immutable
import akka.actor.SchedulerException
import akka.dispatch.MonitorableThreadFactory
import java.util.concurrent.ThreadFactory

class AkkaTimer(wheelShift: Int,
                _tickDuration: FiniteDuration,
                _threadFactory: ThreadFactory) extends AtomicReferenceArray[AkkaTimer.TaskHolder](1 << wheelShift) {
  import AkkaTimer._

  if (wheelShift < 1 || wheelShift > 31)
    throw new IllegalArgumentException("wheelShift must be in the interval [1, 31]")

  /**
   * Override to coordinate your own clock, good for testing etc
   * Must be thread safe
   */
  protected def currentTimeNanos(): Long = System.nanoTime()

  protected def waitNanos(nanos: Long): Unit = {
    // see http://www.javamex.com/tutorials/threads/sleep_issues.shtml
    val sleepMs = if (Helpers.isWindows) (nanos + 4999999) / 10000000 * 10 else (nanos + 999999) / 1000000
    Thread.sleep(sleepMs)
  }

  private val start = currentTimeNanos()
  private val tickDuration = _tickDuration.toNanos
  private val wheelMask = length() - 1
  @volatile private var currentBucket = 0

  def schedule(r: Runnable, delay: FiniteDuration)(implicit ec: ExecutionContext): TimerTask =
    if (stopped != null) {
      throw new SchedulerException("cannot enqueue after shutdown")
    } else if (delay <= Duration.Zero) {
      ec.execute(r)
      NotCancellable
    } else {
      val ticks = (delay.toNanos / tickDuration).toInt
      val rounds = (ticks >> wheelShift).toInt

      /*
       * works as follows:
       * - ticks are calculated to be never “too early”
       * - base off of currentBucket, even after that was moved in the meantime
       * - timer thread will swap in Pause, increment currentBucket, swap in null
       * - hence spin on Pause, else normal CAS
       */
      @tailrec
      def rec(t: TaskHolder): TimerTask = {
        val bucket = (currentBucket + ticks) & wheelMask
        get(bucket) match {
          case Pause ⇒ rec(t)
          case tail ⇒
            t.next = tail
            if (compareAndSet(bucket, tail, t)) t
            else rec(t)
        }
      }

      rec(new TaskHolder(r, null, rounds))
    }

  @volatile private var stopped: Promise[immutable.Seq[TimerTask]] = null
  def stop(): Future[immutable.Seq[TimerTask]] = {
    stopped = Promise()
    stopped.future
  }

  private def clearAll(): immutable.Seq[TimerTask] = {
    def collect(curr: TaskHolder, acc: Vector[TimerTask]): Vector[TimerTask] = {
      curr match {
        case null ⇒ acc
        case x    ⇒ collect(x.next, acc :+ x)
      }
    }
    (0 until length()) flatMap (i ⇒ collect(getAndSet(i, null), Vector.empty))
  }

  // take one thread out of the proffered execution context
  _threadFactory.newThread(new Runnable {
    var tick = 0
    override final def run = blocking { nextTick() } // Signal that we will be blocking
    @tailrec final def nextTick(): Unit = {
      val sleepTime = start + tick * tickDuration - currentTimeNanos()

      if (sleepTime > 0) {
        waitNanos(sleepTime)
      } else {
        // first get the list of tasks out and turn the wheel
        val bucket = currentBucket
        val tasks = getAndSet(bucket, Pause)
        val next = (bucket + 1) & wheelMask
        currentBucket = next
        set(bucket, null)

        // then process the tasks and keep the non-ripe ones in a list
        var last: TaskHolder = null // the last element of the putBack list
        @tailrec def rec1(task: TaskHolder, nonRipe: TaskHolder): TaskHolder = {
          if (task == null) nonRipe
          else if (task.isCancelled) rec1(task.next, nonRipe)
          else if (task.rounds > 0) {
            task.rounds -= 1

            val next = task.next
            task.next = nonRipe

            if (last == null) last = task
            rec1(next, task)
          } else {
            task.executeTask() // TODO do we care if it worked or not?
            rec1(task.next, nonRipe)
          }
        }
        val putBack = rec1(tasks, null)

        // finally put back the non-ripe ones, who had their rounds decremented
        @tailrec def rec2() {
          val tail = get(bucket)
          last.next = tail
          if (!compareAndSet(bucket, tail, putBack)) rec2()
        }
        if (last != null) rec2()

        // and off to the next tick
        tick += 1
      }
      stopped match {
        case null ⇒ nextTick()
        case x    ⇒ x success clearAll()
      }
    }
  }).start()
}

object AkkaTimer {
  private val taskOffset = unsafe.objectFieldOffset(classOf[TaskHolder].getDeclaredField("task"))

  class TaskHolder(@volatile var task: Runnable,
                   @volatile var next: TaskHolder,
                   @volatile var rounds: Int)(
                     implicit executionContext: ExecutionContext) extends TimerTask {
    @tailrec
    private final def extractTask(cancel: Boolean): Runnable = {
      task match {
        case null          ⇒ null // expired
        case CancelledTask ⇒ null // cancelled
        case x ⇒
          if (unsafe.compareAndSwapObject(this, taskOffset, x, if (cancel) CancelledTask else null)) x
          else extractTask(cancel)
      }
    }

    private[akka] final def executeTask(): Boolean = extractTask(cancel = false) match {
      case null | CancelledTask ⇒ false
      case other                ⇒ executionContext execute other; true
    }

    def runDirectly(): Unit = extractTask(cancel = false) match {
      case null ⇒
      case r    ⇒ r.run()
    }

    override def cancel(): Boolean = extractTask(cancel = true) != null

    override def isCancelled: Boolean = task eq CancelledTask
  }

  private val CancelledTask = new Runnable { def run = () }

  private val NotCancellable = new TimerTask {
    def cancel(): Boolean = false
    def isCancelled: Boolean = false
    def runDirectly(): Unit = ()
  }
  // marker object during wheel movement
  private val Pause = new TaskHolder(null, null, 0)(null)
}

trait TimerTask extends Cancellable {
  def runDirectly(): Unit
}