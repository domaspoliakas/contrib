/*
 * Copyright 2021 Precog Data
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

import scala.concurrent.duration.FiniteDuration

/**
 * Provides the capability to rate-limit effects
 */
trait RateLimiting[F[_]] {

  /**
   * Define a new rate limit, returning a set of signals use to communicate with the rate
   * limiter.
   *
   * If invoked with the same `id` multiple times, will always return the [[Signals]] produced
   * by the first invocation.
   *
   * @param id
   *   an identifer for the rate limit, distinct `id`s will return distinct `Signals`, equal
   *   `id`s will return the same `Signals`.
   *
   * @param max
   *   the number of times `limit` may be sequenced within `window` before blocking
   *
   * @param window
   *   the duration within which `limit` applies
   *
   * @param mode
   *   specifies how the usage of the rate limit window is determined
   */
  def rateLimit(
      id: String,
      max: Int,
      window: FiniteDuration,
      mode: RateLimiting.Mode): F[RateLimiting.Signals[F]]
}

object RateLimiting {
  sealed trait Mode

  object Mode {
    /* Rate limits are provided externally, using the `setUsage` signal */
    object External extends Mode
    /* Rate limits are determined by counting the number of requests made */
    object Counting extends Mode
  }

  /**
   * Signals used to communicate with a rate limiter.
   *
   * @param limit
   *   request to be limited, sequencing this effect will block if the rate limit has been
   *   reached, progressing when the rate has reduced below the allowed limit.
   *
   * @param backoff
   *   a synthetic "limit exceeded" signal that causes futher `limit` requests to block as if
   *   the rate limit had been reached. Equivalent to setUsage(max)
   *
   * @param setUsage
   *   allows setting the window usage, allowing an external source (e.g an API) to dictate
   *   limits
   */
  final case class Signals[F[_]](limit: F[Unit], backoff: F[Unit], setUsage: Int => F[Unit])
}
