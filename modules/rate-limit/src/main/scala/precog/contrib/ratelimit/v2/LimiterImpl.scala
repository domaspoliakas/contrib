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

import LimiterImpl._
import cats.effect.Temporal
import cats.effect.kernel.Deferred
import cats.effect.kernel.Unique
import cats.effect.std.MapRef
import cats.effect.std.Queue
import cats.effect.std.Supervisor
import cats.implicits._
import fs2.Stream

case class LimiterImpl[F[_], A](
    limitFunction: LimitFunction[F, A],
    supervisor: Supervisor[F],
    mapRef: MapRef[F, Unique.Token, Option[SupervisedState[F]]],
    queue: Queue[F, Unique.Token],
    limit: Int,
    parallelism: Int
)(implicit F: Temporal[F])
    extends Limiter[F, A] {

  def run =
    Stream.fromQueueUnterminated(queue, limit).parEvalMap(parallelism) { fid =>
      mapRef(fid).get.flatMap {
        case Some(Waiting(signal)) =>
          signal.complete(fid) *> F.pure(println(s"SIG $fid DONE"))
        case _ =>
          // This is a leak? Since signal is never completed, or the caller will destroy the Deferred at submit?
          mapRef(fid).set(Cancelled.some).void
      }
    }

  def lfLoop(fid: Unique.Token, task: F[A]): F[A] = {
    mapRef(fid).get.flatMap {
      case (Some(Cancelled)) =>
        F.canceled.flatMap(_ => F.pure(println("This will be triggered?")) *> task)
      case _ =>
        limitFunction.request.flatMap {
          case Left(nextInstant) =>
            F.realTimeInstant.flatMap { ins =>
              val duration = nextInstant.toEpochMilli - ins.toEpochMilli

              if (duration > 0)
                F.sleep(duration.millis) *> lfLoop(fid, task)
              else
                lfLoop(fid, task)

            }
          case _ => task
        }

    }
  }

  def submit(task: F[A]): F[A] = {

    F.unique.flatMap { fid =>
      Deferred[F, Unique.Token].flatMap { startSignal =>
        val sTask = startSignal.get *> lfLoop(fid, task)

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

object LimiterImpl {

  sealed trait SupervisedState[+F[_]]

  case object Cancelled extends SupervisedState[Nothing]
  case class Waiting[F[_]](signal: Deferred[F, Unique.Token]) extends SupervisedState[F]

}
