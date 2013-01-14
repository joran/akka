/**
 *  Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.actor

import scala.concurrent.duration._
import akka.event.LoggingAdapter
import akka.dispatch.MessageDispatcher
import java.io.Closeable
import java.util.concurrent.atomic.{ AtomicReference, AtomicLong }
import scala.annotation.tailrec
import concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import akka.util.AkkaTimer
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import scala.concurrent.Await
import akka.util.TimerTask
import akka.util.internal.{ HashedWheelTimer, Timeout ⇒ HWTimeout, TimerTask ⇒ HWTimerTask, Timer ⇒ HWTimer }
import akka.dispatch.MonitorableThreadFactory
import scala.util.control.NoStackTrace
import java.util.concurrent.ThreadFactory

case class SchedulerException(msg: String) extends akka.AkkaException(msg) with NoStackTrace

// The Scheduler trait is included in the documentation. KEEP THE LINES SHORT!!!
//#scheduler
/**
 * An Akka scheduler service. This one needs one special behavior: if
 * Closeable, it MUST execute all outstanding tasks upon .close() in order
 * to properly shutdown all dispatchers.
 *
 * Furthermore, this timer service MUST throw IllegalStateException if it
 * cannot schedule a task. Once scheduled, the task MUST be executed. If
 * executed upon close(), the task may execute before its timeout.
 *
 * Scheduler implementation are loaded reflectively at ActorSystem start-up
 * with the following constructor arguments:
 *  - the system’s Config (from system.settings.config)
 *  - a LoggingAdapter
 *  - an ExecutionContext
 *  - a MonitorableThreadFactory
 * It is the scheduler’s choice whether to use the ExecutionContext or the
 * thread factory, but blocking calls (like Thread.sleep) shall be wrapped
 * inside scala.concurrent.blocking {} calls in the former case.
 */
trait Scheduler {
  /**
   * Schedules a message to be sent repeatedly with an initial delay and
   * frequency. E.g. if you would like a message to be sent immediately and
   * thereafter every 500ms you would set delay=Duration.Zero and
   * interval=Duration(500, TimeUnit.MILLISECONDS)
   *
   * Java & Scala API
   */
  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable =
    schedule(initialDelay, interval, new Runnable {
      def run = {
        if (receiver.isTerminated) throw new SchedulerException("timer active for terminated actor")
        receiver ! message
      }
    })

  /**
   * Schedules a function to be run repeatedly with an initial delay and a
   * frequency. E.g. if you would like the function to be run after 2 seconds
   * and thereafter every 100ms you would set delay = Duration(2, TimeUnit.SECONDS)
   * and interval = Duration(100, TimeUnit.MILLISECONDS)
   *
   * Scala API
   */
  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration)(f: ⇒ Unit)(
      implicit executor: ExecutionContext): Cancellable =
    schedule(initialDelay, interval, new Runnable { override def run = f })

  /**
   * Schedules a function to be run repeatedly with an initial delay and
   * a frequency. E.g. if you would like the function to be run after 2
   * seconds and thereafter every 100ms you would set delay = Duration(2,
   * TimeUnit.SECONDS) and interval = Duration(100, TimeUnit.MILLISECONDS)
   *
   * Java API
   */
  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration,
    runnable: Runnable)(implicit executor: ExecutionContext): Cancellable

  /**
   * Schedules a Runnable to be run once with a delay, i.e. a time period that
   * has to pass before the runnable is executed.
   *
   * Java & Scala API
   */
  def scheduleOnce(
    delay: FiniteDuration,
    runnable: Runnable)(implicit executor: ExecutionContext): Cancellable

  /**
   * Schedules a message to be sent once with a delay, i.e. a time period that has
   * to pass before the message is sent.
   *
   * Java & Scala API
   */
  def scheduleOnce(
    delay: FiniteDuration,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable =
    scheduleOnce(delay, new Runnable {
      override def run = {
        if (receiver.isTerminated) throw new SchedulerException("timer active for terminated actor")
        receiver ! message
      }
    })

  /**
   * Schedules a function to be run once with a delay, i.e. a time period that has
   * to pass before the function is run.
   *
   * Scala API
   */
  def scheduleOnce(delay: FiniteDuration)(f: ⇒ Unit)(implicit executor: ExecutionContext): Cancellable =
    scheduleOnce(delay, new Runnable { override def run = f })

  /**
   * The time resolution of this scheduler, i.e. the minimum time interval
   * between executions of a recurring task
   */
  def resolution: FiniteDuration

}
//#scheduler

//#cancellable
/**
 * Signifies something that can be cancelled
 * There is no strict guarantee that the implementation is thread-safe,
 * but it should be good practice to make it so.
 */
trait Cancellable {
  /**
   * Cancels this Cancellable and returns true if that was successful.
   * If this cancellable was (concurrently) cancelled already, then this method
   * will return false although isCancelled will return true.
   *
   * Java & Scala API
   */
  def cancel(): Boolean

  /**
   * Returns true if and only if this Cancellable has been successfully cancelled
   *
   * Java & Scala API
   */
  def isCancelled: Boolean
}
//#cancellable

class AkkaScheduler(config: Config,
                    log: LoggingAdapter,
                    threadFactory: ThreadFactory) extends Scheduler with Closeable {

  val WheelShift = {
    val ticks = config.getInt("akka.scheduler.ticks-per-wheel")
    val shift = 31 - Integer.numberOfLeadingZeros(ticks)
    if ((ticks & ((1 << shift) - 1)) != 0) throw new akka.ConfigurationException("ticks-per-wheel must be a power of 2")
    shift
  }
  val TickDuration = Duration(config.getMilliseconds("akka.scheduler.tick-duration"), MILLISECONDS)
  val ShutdownTimeout = Duration(config.getMilliseconds("akka.scheduler.shutdown-timeout"), MILLISECONDS)

  val timer = new AkkaTimer(WheelShift, TickDuration, threadFactory)

  private val oneNs = Duration.fromNanos(1l)
  private def roundUp(d: FiniteDuration): FiniteDuration =
    try {
      ((d + TickDuration - oneNs) / TickDuration).toLong * TickDuration
    } catch {
      case _: IllegalArgumentException ⇒ d
    }

  override def schedule(initialDelay: FiniteDuration,
                        delay: FiniteDuration,
                        runnable: Runnable)(implicit executor: ExecutionContext): Cancellable =
    new AtomicReference[Cancellable] with Cancellable { self ⇒
      set(timer.schedule(
        new AtomicLong(System.nanoTime + initialDelay.toNanos) with Runnable {
          override def run(): Unit = {
            runnable.run()
            val driftNanos = System.nanoTime - getAndAdd(delay.toNanos)
            try {
              if (self.get != null)
                swap(timer.schedule(this, Duration.fromNanos(Math.max(delay.toNanos - driftNanos, 1))))
            } catch {
              case _: SchedulerException ⇒ // ignore failure to enqueue
            }
          }
        }, roundUp(initialDelay)))

      @tailrec private def swap(c: Cancellable): Unit = {
        get match {
          case null ⇒ if (c != null) c.cancel()
          case old  ⇒ if (!compareAndSet(old, c)) swap(c)
        }
      }

      @tailrec final def cancel(): Boolean = {
        get match {
          case null ⇒ false
          case c ⇒
            if (c.cancel) compareAndSet(c, null)
            else compareAndSet(c, null) || cancel()
        }
      }

      override def isCancelled: Boolean = get == null
    }

  override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext): Cancellable =
    timer.schedule(runnable, roundUp(delay))

  private def execDirectly(t: TimerTask): Unit = {
    try t.runDirectly() catch {
      case e: InterruptedException ⇒ throw e
      case _: SchedulerException   ⇒ // ignore terminated actors
      case e: Exception            ⇒ log.error(e, "exception while executing timer task")
    }
  }

  override def close(): Unit = {
    val tasks = Await.result(timer.stop(), ShutdownTimeout)
    tasks foreach execDirectly
  }

  override def resolution: FiniteDuration = TickDuration
}

/**
 * Scheduled tasks (Runnable and functions) are executed with the supplied dispatcher.
 * Note that dispatcher is by-name parameter, because dispatcher might not be initialized
 * when the scheduler is created.
 *
 * The HashedWheelTimer used by this class MUST throw an IllegalStateException
 * if it does not enqueue a task. Once a task is queued, it MUST be executed or
 * returned from stop().
 */
class DefaultScheduler(config: Config,
                       log: LoggingAdapter,
                       threadFactory: ThreadFactory) extends Scheduler with Closeable {

  val TicksPerWheel = {
    val ticks = config.getInt("akka.scheduler.ticks-per-wheel")
    val shift = 31 - Integer.numberOfLeadingZeros(ticks)
    if ((ticks & ((1 << shift) - 1)) != 0) throw new akka.ConfigurationException("ticks-per-wheel must be a power of 2")
    ticks
  }
  val TickDuration = Duration(config.getMilliseconds("akka.scheduler.tick-duration"), MILLISECONDS)

  val hashedWheelTimer = new HashedWheelTimer(log, threadFactory, TickDuration, TicksPerWheel)

  override def schedule(initialDelay: FiniteDuration,
                        delay: FiniteDuration,
                        receiver: ActorRef,
                        message: Any)(implicit executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable = {
    val continuousCancellable = new ContinuousCancellable
    continuousCancellable.init(
      hashedWheelTimer.newTimeout(
        new AtomicLong(System.nanoTime + initialDelay.toNanos) with HWTimerTask with ContinuousScheduling {
          def run(timeout: HWTimeout) {
            executor execute new Runnable {
              override def run = {
                receiver ! message
                // Check if the receiver is still alive and kicking before reschedule the task
                if (receiver.isTerminated) log.debug("Could not reschedule message to be sent because receiving actor {} has been terminated.", receiver)
                else {
                  val driftNanos = System.nanoTime - getAndAdd(delay.toNanos)
                  scheduleNext(timeout, Duration.fromNanos(Math.max(delay.toNanos - driftNanos, 1)), continuousCancellable)
                }
              }
            }
          }
        },
        initialDelay))
  }

  override def schedule(initialDelay: FiniteDuration,
                        delay: FiniteDuration)(f: ⇒ Unit)(implicit executor: ExecutionContext): Cancellable =
    schedule(initialDelay, delay, new Runnable { override def run = f })

  override def schedule(initialDelay: FiniteDuration,
                        delay: FiniteDuration,
                        runnable: Runnable)(implicit executor: ExecutionContext): Cancellable = {
    val continuousCancellable = new ContinuousCancellable
    continuousCancellable.init(
      hashedWheelTimer.newTimeout(
        new AtomicLong(System.nanoTime + initialDelay.toNanos) with HWTimerTask with ContinuousScheduling {
          override def run(timeout: HWTimeout): Unit = executor.execute(new Runnable {
            override def run = {
              runnable.run()
              val driftNanos = System.nanoTime - getAndAdd(delay.toNanos)
              scheduleNext(timeout, Duration.fromNanos(Math.max(delay.toNanos - driftNanos, 1)), continuousCancellable)
            }
          })
        },
        initialDelay))
  }

  override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext): Cancellable =
    new DefaultCancellable(
      hashedWheelTimer.newTimeout(
        new HWTimerTask() { def run(timeout: HWTimeout): Unit = executor.execute(runnable) },
        delay))

  override def scheduleOnce(delay: FiniteDuration, receiver: ActorRef, message: Any)(implicit executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable =
    scheduleOnce(delay, new Runnable { override def run = receiver ! message })

  override def scheduleOnce(delay: FiniteDuration)(f: ⇒ Unit)(implicit executor: ExecutionContext): Cancellable =
    scheduleOnce(delay, new Runnable { override def run = f })

  private trait ContinuousScheduling { this: HWTimerTask ⇒
    def scheduleNext(timeout: HWTimeout, delay: FiniteDuration, delegator: ContinuousCancellable) {
      try delegator.swap(timeout.getTimer.newTimeout(this, delay)) catch { case _: IllegalStateException ⇒ } // stop recurring if timer is stopped
    }
  }

  private def execDirectly(t: HWTimeout): Unit = {
    try t.getTask.run(t) catch {
      case e: InterruptedException ⇒ throw e
      case e: Exception            ⇒ log.error(e, "exception while executing timer task")
    }
  }

  override def close(): Unit = {
    val i = hashedWheelTimer.stop().iterator()
    while (i.hasNext) execDirectly(i.next())
  }

  override def resolution: FiniteDuration = TickDuration
}

private[akka] object ContinuousCancellable {
  val initial: HWTimeout = new HWTimeout {
    override def getTimer: HWTimer = null
    override def getTask: HWTimerTask = null
    override def isExpired: Boolean = false
    override def isCancelled: Boolean = false
    override def cancel: Boolean = true
  }

  val cancelled: HWTimeout = new HWTimeout {
    override def getTimer: HWTimer = null
    override def getTask: HWTimerTask = null
    override def isExpired: Boolean = false
    override def isCancelled: Boolean = true
    override def cancel: Boolean = false
  }
}
/**
 * Wrapper of a [[org.jboss.netty.akka.util.Timeout]] that delegates all
 * methods. Needed to be able to cancel continuous tasks,
 * since they create new Timeout for each tick.
 */
private[akka] class ContinuousCancellable extends AtomicReference[HWTimeout](ContinuousCancellable.initial) with Cancellable {
  private[akka] def init(initialTimeout: HWTimeout): this.type = {
    compareAndSet(ContinuousCancellable.initial, initialTimeout)
    this
  }

  @tailrec private[akka] final def swap(newTimeout: HWTimeout): Unit = get match {
    case some if some.isCancelled ⇒ try cancel() finally newTimeout.cancel()
    case some                     ⇒ if (!compareAndSet(some, newTimeout)) swap(newTimeout)
  }

  def isCancelled(): Boolean = get().isCancelled()
  def cancel(): Boolean = getAndSet(ContinuousCancellable.cancelled).cancel()
}

private[akka] class DefaultCancellable(timeout: HWTimeout) extends AtomicReference[HWTimeout](timeout) with Cancellable {
  override def cancel(): Boolean = getAndSet(ContinuousCancellable.cancelled).cancel()
  override def isCancelled: Boolean = get().isCancelled
}
