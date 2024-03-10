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
import cats.Functor
import cats.effect.Resource
import cats.effect.Temporal
import cats.syntax.all._
import cats.~>
import io.chrisdavenport.rediculous.Redis
import io.chrisdavenport.rediculous.RedisCommands
import io.chrisdavenport.rediculous.RedisConnection
import io.chrisdavenport.rediculous.RedisCtx
import io.chrisdavenport.rediculous.RedisTransaction
import precog.contrib.ratelimit.RateLimiting.Mode
import retry.RetryDetails
import retry.RetryPolicy

object RedisRateLimiting {
  def apply[F[_]: Temporal](
      connection: Resource[F, RedisConnection[F]],
      retryPolicy: RetryPolicy[F],
      onError: (Throwable, RetryDetails) => F[Unit]): RateLimiting[F] =
    new RateLimiting[F] {
      val executeRetrying: Redis[F, *] ~> F =
        λ[Redis[F, *] ~> F] { red =>
          retry.retryingOnAllErrors(retryPolicy, onError)(
            connection.use(conn => red.run(conn))
          )
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

    assert(window >= 1.milli, "window must be at least 1 millisecond")

    val nowF: Redis[F, FiniteDuration] =
      Redis.liftF(F.realTime)

    def containingWindowEndsAtMillis(ts: FiniteDuration, window: FiniteDuration): Long =
      (ts.toMillis / window.toMillis + 1) * window.toMillis

    def bucket(endsAtMillis: Long): String =
      s"${key}:${endsAtMillis}"

    def bucketExpiresIn(endsAtMillis: Long, now: FiniteDuration): FiniteDuration =
      endsAtMillis.millis + 1.second - now

    def transact[A](txn: RedisTransaction[A]): Redis[F, A] =
      txn.transact[F].flatMap {
        case RedisTransaction.TxResult.Success(value) =>
          value.pure[Redis[F, *]]
        case RedisTransaction.TxResult.Aborted =>
          Redis.liftF(F.raiseError[A](new Throwable("Transaction Aborted")))
        case RedisTransaction.TxResult.Error(value) =>
          Redis.liftF(F.raiseError[A](new Throwable(s"Transaction Failed: $value")))
      }

    def setRemaining[G[_]: Functor: RedisCtx](
        endsAtMillis: Long,
        expiresIn: FiniteDuration,
        remaining: Int,
        ifNotExists: Boolean): G[Unit] = {

      val opts =
        RedisCommands
          .SetOpts
          .default
          .copy(
            setMilliseconds = Some(expiresIn.toMillis),
            setCondition = Option.when(ifNotExists)(RedisCommands.Condition.Nx)
          )

      RedisCommands.set[G](bucket(endsAtMillis), remaining.toString, opts).void
    }

    def createBucketIfNotExists(
        endsAtMillis: Long,
        expiresIn: FiniteDuration): RedisTransaction[Unit] =
      setRemaining[RedisTransaction](endsAtMillis, expiresIn, max, ifNotExists = true)

    def decrementAndGetRemaining(
        endsAtMillis: Long,
        expiresIn: FiniteDuration): Redis[F, Long] =
      transact(
        createBucketIfNotExists(endsAtMillis, expiresIn) *>
          RedisCommands.decr[RedisTransaction](bucket(endsAtMillis)))

    def getRemaining(endsAtMillis: Long, expiresIn: FiniteDuration): Redis[F, Long] =
      transact(
        createBucketIfNotExists(endsAtMillis, expiresIn) *>
          RedisCommands.get[RedisTransaction](bucket(endsAtMillis))
      ).flatMap { response =>
        Redis.liftF {
          response
            .flatMap(_.toLongOption)
            .liftTo[F](new Throwable(s"Unexpected error getting rate limit bucket '${bucket(
              endsAtMillis)}' from redis: expected some long, got ${response}"))
        }
      }

    def setWindowUsage(usage: Int): Redis[F, Unit] =
      nowF.flatMap { now =>
        val currentBucketEndsAtMillis = containingWindowEndsAtMillis(now, window)
        setRemaining[Redis[F, *]](
          currentBucketEndsAtMillis,
          bucketExpiresIn(currentBucketEndsAtMillis, now),
          max - usage,
          ifNotExists = false)
      }

    val backoff: Redis[F, Unit] = setWindowUsage(max)

    lazy val limit: Redis[F, Unit] =
      nowF.flatMap { now =>
        val currentBucketEndsAtMillis = containingWindowEndsAtMillis(now, window)
        val currentBucketExpiresIn = bucketExpiresIn(currentBucketEndsAtMillis, now)

        val reqsRemaining = mode match {
          case Mode.Counting =>
            decrementAndGetRemaining(currentBucketEndsAtMillis, currentBucketExpiresIn).map(
              _ + 1)
          case Mode.External => getRemaining(currentBucketEndsAtMillis, currentBucketExpiresIn)
        }

        reqsRemaining.flatMap { remaining =>
          Applicative[Redis[F, *]].whenA(remaining <= 0) {
            Redis.liftF(F.sleep(currentBucketEndsAtMillis.millis - now)) >> limit
          }
        }
      }

    RateLimiting.Signals(limit = limit, backoff = backoff, setWindowUsage)
  }
}
