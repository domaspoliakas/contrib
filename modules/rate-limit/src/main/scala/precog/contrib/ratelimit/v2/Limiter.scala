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

import cats.effect.Resource

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
}
