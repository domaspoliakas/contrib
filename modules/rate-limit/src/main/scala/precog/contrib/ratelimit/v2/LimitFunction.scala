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

import java.time.Instant

import cats.Semigroup
import cats.effect.Spawn
import cats.effect.instances.all._
import cats.syntax.all._
import cats.~>
import org.typelevel.cats.time._

trait LimitFunction[F[_], -A] { self =>

  /**
   * Request to execute a task, subject to the limit defined by this function.
   *
   * Returns `Right` to indicate the task may be executed now along with an effect to sequence
   * if the task is not, executed for any reason and `Left` to indicate the limit has been
   * reached along with a time in the future at which `request` may be attempted again.
   */
  def request: F[Option[Instant]]

  /**
   * Update the state of this limit function based on the value produced by the task.
   *
   * @param a
   *   the task result
   */
  def update(a: A): F[Unit]

  /**
   * Transforms the effect type of this limit function using `f`.
   */
  def mapK[G[_]](f: F ~> G): LimitFunction[G, A] =
    new LimitFunction[G, A] {
      def request = f(self.request)
      def update(a: A) = f(self.update(a))
    }
}

object LimitFunction {
  implicit def semigroup[F[_], A](implicit F: Spawn[F]): Semigroup[LimitFunction[F, A]] =
    new Semigroup[LimitFunction[F, A]] {
      def combine(l: LimitFunction[F, A], r: LimitFunction[F, A]): LimitFunction[F, A] =
        new LimitFunction[F, A] {
          def bothDeferringErrors(a: F[Unit], b: F[Unit]): F[Unit] =
            (a.attempt, b.attempt).parMapN(_ *> _).rethrow

          def request: F[Option[Instant]] =
            F.uncancelable { poll =>
              poll(F.both(l.request, r.request)).flatMap {
                case (Some(a), Some(b)) => F.pure(Some(a max b))
                case (Some(a), _) => F.pure(Some(a))
                case (_, Some(b)) => F.pure(Some(b))
                case _ => F.pure(None)
              }
            }

          def update(a: A): F[Unit] =
            bothDeferringErrors(l.update(a), r.update(a))
        }
    }
}
