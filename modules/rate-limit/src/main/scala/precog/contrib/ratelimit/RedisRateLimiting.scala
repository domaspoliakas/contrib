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

package precog.contrib.ratelimit

import scala.concurrent.duration._

import cats.Applicative
import cats.effect.Temporal
import cats.syntax.all._
import cats.~>
import io.chrisdavenport.rediculous.Redis
import io.chrisdavenport.rediculous.RedisCommands
import io.chrisdavenport.rediculous.RedisConnection
import io.chrisdavenport.rediculous.RedisTransaction
import precog.contrib.ratelimit.RateLimiting.Mode.Counting
import precog.contrib.ratelimit.RateLimiting.Mode.External
import retry.RetryDetails
import retry.RetryPolicy

object RedisRateLimiting {
  def apply[F[_]: Temporal](
      connection: RedisConnection[F],
      retryPolicy: RetryPolicy[F],
      onError: (Throwable, RetryDetails) => F[Unit]): RateLimiting[F] =
    new RateLimiting[F] {
      val executeRetrying: Redis[F, *] ~> F =
        λ[Redis[F, *] ~> F] { red =>
          retry.retryingOnAllErrors(retryPolicy, onError)(red.run(connection))
        }

      def rateLimit(
          id: String,
          max: Int,
          window: FiniteDuration,
          mode: RateLimiting.Mode): F[RateLimiting.Signals[F]] =
        signals[F](id, max, window, mode).mapK(executeRetrying).pure[F]
    }

  def signals[F[_]](key: String, max: Int, window: FiniteDuration, mode: RateLimiting.Mode)(
      implicit F: Temporal[F]): RateLimiting.Signals[Redis[F, *]] = {

    val nowF: Redis[F, FiniteDuration] =
      Redis.liftF(F.realTime)

    def containingWindowEndsAt(ts: FiniteDuration, window: FiniteDuration): Long =
      (ts.toSeconds / window.toSeconds + 1) * window.toSeconds

    def bucket(endsAtSecs: Long): String =
      s"${key}:${endsAtSecs}"

    def transact[A](txn: RedisTransaction[A]): Redis[F, A] =
      txn.transact[F].flatMap {
        case RedisTransaction.TxResult.Success(value) =>
          value.pure[Redis[F, *]]
        case RedisTransaction.TxResult.Aborted =>
          Redis.liftF(F.raiseError[A](new Throwable("Transaction Aborted")))
        case RedisTransaction.TxResult.Error(value) =>
          Redis.liftF(F.raiseError[A](new Throwable(s"Transaction Failed: $value")))
      }

    def createBucketIfNotExists(
        endsAtSecs: Long,
        expiresIn: FiniteDuration): RedisTransaction[Unit] =
      RedisCommands
        .set[RedisTransaction](
          bucket(endsAtSecs),
          max.toString,
          RedisCommands
            .SetOpts
            .default
            .copy(
              setSeconds = Some(expiresIn.toSeconds),
              setCondition = Some(RedisCommands.Condition.Nx))
        )
        .void

    def decrementAndGetRemaining(endsAtSecs: Long, expiresIn: FiniteDuration): Redis[F, Long] =
      transact(
        createBucketIfNotExists(endsAtSecs, expiresIn) *>
          RedisCommands.decr[RedisTransaction](bucket(endsAtSecs)))

    def getRemaining(endsAtSecs: Long, expiresIn: FiniteDuration): Redis[F, Long] =
      transact(
        createBucketIfNotExists(endsAtSecs, expiresIn) *>
          RedisCommands.get[RedisTransaction](bucket(endsAtSecs))
      ).flatMap { response =>
        Redis.liftF {
          response
            .flatMap(_.toLongOption)
            .liftTo[F](new Throwable(s"Unexpected error getting rate limit bucket '${bucket(
              endsAtSecs)}' from redis: expected some long, got ${response}"))
        }
      }

    def setRemaining(
        endsAtSecs: Long,
        expiresIn: FiniteDuration,
        remaining: Int): Redis[F, Unit] =
      RedisCommands
        .set[Redis[F, *]](
          bucket(endsAtSecs),
          remaining.toString,
          RedisCommands.SetOpts.default.copy(setSeconds = Some(expiresIn.toSeconds)))
        .void

    def setWindowUsage(usage: Int): Redis[F, Unit] =
      nowF.flatMap { now =>
        val currentBucketEndsAt = containingWindowEndsAt(now, window)
        val currentBucketExpiresIn = (currentBucketEndsAt + 1).seconds - now
        setRemaining(currentBucketEndsAt, currentBucketExpiresIn, max - usage)
      }

    val backoff: Redis[F, Unit] = setWindowUsage(max)

    lazy val limit: Redis[F, Unit] =
      nowF.flatMap { now =>
        val currentBucketEndsAt = containingWindowEndsAt(now, window)
        val currentBucketExpiresIn = (currentBucketEndsAt + 1).seconds - now

        val reqsRemaining = mode match {
          case Counting =>
            decrementAndGetRemaining(currentBucketEndsAt, currentBucketExpiresIn).map(_ + 1)
          case External => getRemaining(currentBucketEndsAt, currentBucketExpiresIn)
        }

        reqsRemaining.flatMap { remaining =>
          Applicative[Redis[F, *]].whenA(remaining <= 0) {
            Redis.liftF(F.sleep(currentBucketExpiresIn - 1.second)) >> limit
          }
        }
      }

    RateLimiting.Signals(limit = limit, backoff = backoff, setWindowUsage)
  }
}
