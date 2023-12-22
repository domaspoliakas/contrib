/*
 * Copyright 2022 Precog Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package precog.contrib.ratelimit.v2

import scala.concurrent.duration._

import cats.effect.Async
import cats.effect.Fiber
import cats.effect.Resource
import cats.effect.Temporal
import cats.effect.kernel.Deferred
import cats.effect.kernel.Outcome
import cats.effect.kernel.Unique
import cats.effect.std.MapRef
import cats.effect.std.Queue
import cats.effect.std.Supervisor
import cats.implicits._
import fs2.Stream

trait Limiter[F[_], A] {

  /**
   * Submit a task to be executed, subject to the limit.
   *
   * Designed to be called concurrently, tasks will be executed as fast as the limit allows and
   * then queued when the limit is reached and executed FIFO as capacity becomes available.
   *
   * Cancelation of the returned effect is respected and will either cancel the submitted task
   * if currently running or prevent it from being run, if it is enqueued at the time
   * cancelation is observed.
   */
  def submit(task: F[A]): F[A]
}

object Limiter {
  def forFunction[F[_], A](lf: LimitFunction[F, A]): Resource[F, Limiter[F, A]] =
    ???

  def noOp[F[_], A]: Limiter[F, A] =
    new Limiter[F, A] {
      def submit(task: F[A]) = task
    }

  def limiter[F[_], A](
      limitFunction: LimitFunction[F, A],
      limit: Int,
      parallelism: Int
  )(implicit F: Async[F]): Resource[F, Limiter[F, A]] = {

    for {
      supervisor <- Supervisor[F]
      mapRef <- Resource.eval(
        MapRef.ofScalaConcurrentTrieMap[F, Unique.Token, SupervisedState[F, A]])
      queue <- Resource.eval(Queue.unbounded[F, Unique.Token])
      _ <-
        F.background(
          Stream
            .fromQueueUnterminated(queue, limit)
            .parEvalMap(parallelism) { fid =>
              def req: F[Unit] = mapRef(fid).modify {
                case s @ Some(Waiting(task, signal)) =>
                  val next = limitFunction.request.flatMap {
                    case Left(i) =>
                      F.realTimeInstant.flatMap { now =>
                        val sleepTime = i.toEpochMilli - now.toEpochMilli
                        if (sleepTime > 0)
                          F.sleep(sleepTime.millis) *> req
                        else
                          req
                      }

                    case Right(_) =>
                      supervisor
                        .supervise(
                          task
                        )
                        .flatMap(fiber =>
                          mapRef(fid).modify {
                            case None => (None, fiber.cancel)
                            case _ => (Running(fiber).some, F.unit)
                          } *>
                            fiber.join.flatMap(signal.complete))
                        .void

                  }

                  (s, next)

                case s =>
                  (s, F.unit)

              }.flatten

              req
            }
            .compile
            .drain
        )

    } yield LimiterImpl(supervisor, mapRef, queue)

  }

  private case class LimiterImpl[F[_], A](
      supervisor: Supervisor[F],
      mapRef: MapRef[F, Unique.Token, Option[SupervisedState[F, A]]],
      queue: Queue[F, Unique.Token]
  )(implicit F: Temporal[F])
      extends Limiter[F, A] {

    def submit(task: F[A]): F[A] = {

      F.unique.flatMap { fid =>
        Deferred[F, Outcome[F, Throwable, A]].flatMap { signal =>
          val cancellation: F[Unit] = mapRef(fid).modify {
            case Some(Running(fiber)) =>
              (None, fiber.cancel)
            case Some(Waiting(_, signal)) =>
              (None, signal.complete(Outcome.canceled).void)
            case _ => (None, F.unit)
          }.flatten

          val pipeline =
            mapRef(fid)
              .set(Waiting[F, A](task, signal).some)
              .flatMap(_ => queue.offer(fid))
              .flatMap(_ => signal.get.flatMap(_.embedError))

          F.onCancel(
            pipeline,
            cancellation
          )

        }
      }

    }

  }

  sealed trait SupervisedState[+F[A], +A]

  case class Waiting[F[_], A](task: F[A], signal: Deferred[F, Outcome[F, Throwable, A]])
      extends SupervisedState[F, A]
  case class Running[F[_], A](fiber: Fiber[F, Throwable, A]) extends SupervisedState[F, A]

}
