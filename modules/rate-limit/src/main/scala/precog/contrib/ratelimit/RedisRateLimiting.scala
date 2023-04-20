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

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

import cats.effect.Async
import cats.syntax.all._
import io.chrisdavenport.rediculous.Redis
import io.chrisdavenport.rediculous.RedisCommands
import io.chrisdavenport.rediculous.RedisConnection
import io.chrisdavenport.rediculous.RedisTransaction

class RedisRateLimiting[F[_], A](conn: RedisConnection[F])(implicit F: Async[F])
    extends RateLimiting[F] {

  override def rateLimit(
      key: String,
      max: Int,
      window: FiniteDuration,
      mode: RateLimiting.Mode): F[RateLimiting.Signals[F]] =
    RateLimiting
      .Signals[F](
        limit(key, max, window),
        backoff(key, max, window),
        setUsage(key, max, window))
      .pure[F]

  private def setUsage(key: String, max: Int, window: FiniteDuration)(usage: Int): F[Unit] =
    setWindowUsage(key, max, window)(usage)

  private def setWindowUsage(key: String, max: Int, window: FiniteDuration)(
      usage: Int): F[Unit] =
    for {
      now <- nowF
      stableEndEpochSec = stableEnd(now, window)
      expire = FiniteDuration(stableEndEpochSec + 1, TimeUnit.SECONDS) - now
      _ <- set(key, expire.toSeconds, stableEndEpochSec, max - usage)
    } yield ()

  private def backoff(key: String, max: Int, window: FiniteDuration): F[Unit] =
    setWindowUsage(key, max, window)(max)

  private def limit(key: String, max: Int, window: FiniteDuration): F[Unit] =
    for {
      now <- nowF
      stableEndEpochSec = stableEnd(now, window)
      reqsRemaining <- decr(key, stableEndEpochSec, max)
      _ <-
        F.whenA(reqsRemaining < 0)(
          F.sleep(
            FiniteDuration(
              (stableEndEpochSec * 1000) - now.toMillis,
              TimeUnit.MILLISECONDS)) >> limit(key, max, window))
    } yield ()

  private def stableEnd(now: FiniteDuration, window: FiniteDuration): Long =
    (now.toSeconds / window.toSeconds + 1) * window.toSeconds

  private val nowF: F[FiniteDuration] = F.realTime

  private def decr(key: String, endEpochSec: Long, max: Int): F[Long] = {
    val ops =
      (
        RedisCommands.setnx[RedisTransaction](s"$key:$endEpochSec", max.toString()),
        RedisCommands.decr[RedisTransaction](s"$key:$endEpochSec")
      ).mapN {
        case (_, i) =>
          i
      }

    ops.transact[F].run(conn).flatMap {
      case RedisTransaction.TxResult.Success(value) => value.pure[F]
      case RedisTransaction.TxResult.Aborted =>
        Async[F].raiseError[Long](new Throwable("Transaction Aborted"))
      case RedisTransaction.TxResult.Error(value) =>
        Async[F].raiseError[Long](new Throwable(s"Transaction Raised Error $value"))

    }
  }

  private def set(key: String, expireSec: Long, endEpochSec: Long, i: Int): F[Unit] = {
    val op =
      RedisCommands.set[Redis[F, *]](s"$key:$endEpochSec", i.toString(), RedisCommands.SetOpts.default.copy(setSeconds = expireSec.some)).void

    op.run(conn)
  }

}
