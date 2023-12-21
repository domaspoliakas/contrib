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

import cats.effect.Resource
import cats.effect.Temporal
import cats.effect.kernel.Deferred
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
      supervisor: Supervisor[F],
      mapRef: MapRef[F, Unique.Token, Option[SupervisedState[F]]],
      queue: Queue[F, Unique.Token],
      limit: Int,
      parallelism: Int
  )(implicit F: Temporal[F]): Resource[F, Limiter[F, A]] = {

    val stream = Stream.fromQueueUnterminated(queue, limit).parEvalMap(parallelism) { fid =>
      mapRef(fid).get.flatMap {
        case Some(Waiting(signal)) =>
          signal.complete(fid) *> F.pure(println(s"SIG $fid DONE"))
        case _ =>
          // This is a leak? Since signal is never completed, or the caller will destroy the Deferred at submit?
          mapRef(fid).set(Cancelled.some).void
      }
    }

    Stream(LimiterImpl(limitFunction, supervisor, mapRef, queue))
      .concurrently(stream)
      .compile
      .resource
      .lastOrError
  }

  private case class LimiterImpl[F[_], A](
      limitFunction: LimitFunction[F, A],
      supervisor: Supervisor[F],
      mapRef: MapRef[F, Unique.Token, Option[SupervisedState[F]]],
      queue: Queue[F, Unique.Token]
  )(implicit F: Temporal[F])
      extends Limiter[F, A] {

    def submit(task: F[A]): F[A] = {

      F.unique.flatMap { fid =>
        Deferred[F, Unique.Token].flatMap { startSignal =>
          val sTask = startSignal.get *> task // *> ??? // lfLoop(fid, task)

          val pipeline = for {
            f <- supervisor.supervise(
              sTask
            ) // Run and Forget, semantically blocked until the streaming takes from Queue
            _ <- mapRef(fid)
              .getAndSet(Waiting[F](startSignal).some)
              .flatMap(_ => queue.offer(fid))
            res <- f.join.flatMap(_.embedError)

          } yield res

          F.onCancel(pipeline, mapRef(fid).set(Cancelled.some))

        }
      }

    }

  }

  sealed trait SupervisedState[+F[_]]

  case object Cancelled extends SupervisedState[Nothing]
  case class Waiting[F[_]](signal: Deferred[F, Unique.Token]) extends SupervisedState[F]

}
