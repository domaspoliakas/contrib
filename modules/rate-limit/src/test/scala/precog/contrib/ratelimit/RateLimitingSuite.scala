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

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

import cats.effect.IO
import cats.effect.Resource
import cats.effect.testkit.TestControl
import mongo4cats.testkit.RedisTestkit
import precog.contrib.ratelimit.LocalRateLimiting
import precog.contrib.ratelimit.RateLimiting
import precog.contrib.ratelimit.RedisRateLimiting

class LocalRateLimitingSuite
    extends RateLimitingSuite(LocalRateLimiting[IO], TestControl.executeEmbed(_))

class RedisRateLimitingSuite
    extends RateLimitingSuite(
      RedisTestkit.connection.map(RedisRateLimiting[IO](_)),
      identity(_)
    )

abstract class RateLimitingSuite(
    rateLimiting: Resource[IO, RateLimiting[IO]],
    testControl: IO[Unit] => IO[Unit])
    extends munit.CatsEffectSuite {

  val tinyDuration = FiniteDuration(70, TimeUnit.MILLISECONDS)
  val windowDuration = FiniteDuration(2, TimeUnit.SECONDS)

  test("counting: sequence of limits works") {
    testControl {
      rateLimiting.use { rl =>
        for {
          signals <- rl.rateLimit("id", 2, windowDuration, RateLimiting.Mode.Counting)
          _ <- sleepToBeginWindow
          _ <- signals.limit
          nowInit0 <- IO.realTime
          _ <- signals.limit
          nowInit1 <- IO.realTime
          _ <- signals.limit
          now0 <- IO.realTime
          _ <- signals.limit
          now1 <- IO.realTime
          _ <- signals.limit
          now2 <- IO.realTime
          _ <- signals.limit
          now3 <- IO.realTime
          _ <- signals.limit
          now4 <- IO.realTime
          _ <- signals.limit
          now5 <- IO.realTime
          _ <- signals.limit
          now6 <- IO.realTime
        } yield {
          assertInitiating("nowInit1 - nowInit0", nowInit1 - nowInit0)
          assertInitiating("now0 - nowInit1", now0 - nowInit1)
          assertTiny("now1 - now0", now1 - now0)
          assertWaitWindow("now2 - now1", now2 - now1)
          assertTiny("now3 - now2", now3 - now2)
          assertWaitWindow("now4 - now3", now4 - now3)
          assertTiny("now5 - now4", now5 - now4)
          assertWaitWindow("now6 - now5", now6 - now5)
        }
      }
    }
  }

  test("counting: backoff works") {
    testControl {
      rateLimiting.use { rl =>
        for {
          signals <- rl.rateLimit("id", 4, windowDuration, RateLimiting.Mode.Counting)
          _ <- sleepToBeginWindow
          _ <- signals.backoff
          now0 <- IO.realTime
          _ <- signals.limit
          now1 <- IO.realTime
          _ <- signals.backoff
          now2 <- IO.realTime
          _ <- signals.limit
          now3 <- IO.realTime
        } yield {
          assertWaitWindow("now1 - now0", now1 - now0)
          assertTiny("now2 - now1", now2 - now1)
          assertWaitWindow("now3 - now2", now3 - now2)
        }
      }
    }
  }

  test("counting: setUsage high rate limits accordingly") {
    testControl {
      rateLimiting.use { rl =>
        for {
          signals <- rl.rateLimit("id", 10, windowDuration, RateLimiting.Mode.Counting)
          _ <- sleepToBeginWindow
          _ <- signals.setUsage(9)
          now0 <- IO.realTime
          _ <- signals.limit
          now1 <- IO.realTime
          _ <- signals.limit
          now2 <- IO.realTime
          _ <- signals.limit
          _ <- signals.limit
          _ <- signals.limit
          _ <- signals.limit
          _ <- signals.setUsage(8)
          _ <- signals.limit
          _ <- signals.limit
          now3 <- IO.realTime
          _ <- signals.limit
          now4 <- IO.realTime
        } yield {
          assertTiny("now1 - now0", now1 - now0)
          assertWaitWindow("now2 - now1", now2 - now1)
          assertTiny("now3 - now2", now3 - now2)
          assertWaitWindow("now4 - now3", now4 - now3)
        }
      }
    }
  }

  test("counting: setUsage low defers rate limits accordingly") {
    testControl {
      rateLimiting.use { rl =>
        for {
          signals <- rl.rateLimit("id", 3, windowDuration, RateLimiting.Mode.Counting)
          _ <- sleepToBeginWindow
          _ <- signals.limit
          now0 <- IO.realTime
          _ <- signals.limit
          now1 <- IO.realTime
          _ <- signals.setUsage(1)
          _ <- signals.limit
          _ <- signals.limit
          now2 <- IO.realTime
          _ <- signals.setUsage(0)
          _ <- signals.limit
          _ <- signals.limit
          _ <- signals.limit
          now3 <- IO.realTime
          _ <- signals.setUsage(0)
          _ <- signals.limit
          _ <- signals.limit
          _ <- signals.limit
          now4 <- IO.realTime
        } yield {
          assertTiny("now1 - now0", now1 - now0)
          assertTiny("now2 - now1", now2 - now1)
          assertTiny("now3 - now2", now3 - now2)
          assertTiny("now4 - now3", now4 - now3)
        }
      }
    }
  }

  test("external: limit calls do not affect usage") {
    testControl {
      rateLimiting.use { rl =>
        for {
          signals <- rl.rateLimit("id", 1, windowDuration, RateLimiting.Mode.External)
          _ <- sleepToBeginWindow
          _ <- signals.limit
          nowInit0 <- IO.realTime
          _ <- signals.limit
          nowInit1 <- IO.realTime
          _ <- signals.limit
          now0 <- IO.realTime
          _ <- signals.limit
          _ <- signals.limit
          _ <- signals.limit
          _ <- signals.limit
          now1 <- IO.realTime
        } yield {
          assertInitiating("nowInit1 - nowInit0", nowInit1 - nowInit0)
          assertInitiating("now0 - nowInit1", now0 - nowInit1)
          assertTiny("now1 - now0", now1 - now0)
        }
      }
    }
  }

  test("external: backoff works") {
    testControl {
      rateLimiting.use { rl =>
        for {
          signals <- rl.rateLimit("id", 4, windowDuration, RateLimiting.Mode.External)
          _ <- sleepToBeginWindow
          now0 <- IO.realTime
          _ <- signals.backoff
          _ <- signals.limit
          now1 <- IO.realTime
          _ <- signals.backoff
          now2 <- IO.realTime
          _ <- signals.limit
          now3 <- IO.realTime
        } yield {
          assertWaitWindow("now1 - now0", now1 - now0, tiny = tinyDuration * 2)
          assertTiny("now2 - now1", now2 - now1)
          assertWaitWindow("now3 - now2", now3 - now2, tiny = tinyDuration * 2)
        }
      }
    }
  }

  test("external: setUsage to max rate limits accordingly") {
    testControl {
      rateLimiting.use { rl =>
        for {
          signals <- rl.rateLimit("id", 10, windowDuration, RateLimiting.Mode.External)
          _ <- sleepToBeginWindow
          now0 <- IO.realTime
          _ <- signals.setUsage(10)
          _ <- signals.limit
          now1 <- IO.realTime
          _ <- signals.setUsage(10)
          now2 <- IO.realTime
          _ <- signals.limit
          now3 <- IO.realTime
        } yield {
          assertWaitWindow("now1 - now0", now1 - now0, tiny = tinyDuration * 2)
          assertTiny("now2 - now1", now2 - now1)
          assertWaitWindow("now3 - now2", now3 - now2, tiny = tinyDuration * 2)
        }
      }
    }
  }

  test("external: setUsage below max does not rate limit") {
    testControl {
      rateLimiting.use { rl =>
        for {
          signals <- rl.rateLimit("id", 3, windowDuration, RateLimiting.Mode.External)
          _ <- sleepToBeginWindow
          _ <- signals.limit
          now0 <- IO.realTime
          _ <- signals.limit
          now1 <- IO.realTime
          _ <- signals.setUsage(1)
          _ <- signals.limit
          _ <- signals.limit
          now2 <- IO.realTime
          _ <- signals.setUsage(2)
          _ <- signals.limit
          _ <- signals.limit
          _ <- signals.limit
          now3 <- IO.realTime
          _ <- signals.setUsage(0)
          _ <- signals.limit
          _ <- signals.limit
          _ <- signals.limit
          now4 <- IO.realTime
        } yield {
          assertTiny("now1 - now0", now1 - now0)
          assertTiny("now2 - now1", now2 - now1)
          assertTiny("now3 - now2", now3 - now2)
          assertTiny("now4 - now3", now4 - now3)
        }
      }
    }
  }

  private def sleepToBeginWindow =
    IO.realTime
      .flatMap(now => IO.sleep(stableEnd(now, windowDuration) - now + (tinyDuration / 5)))

  private def stableEnd(now: FiniteDuration, window: FiniteDuration): FiniteDuration = {
    FiniteDuration(
      ((now.toSeconds / window.toSeconds) + 1) * window.toSeconds,
      TimeUnit.SECONDS)
  }

  def assertTiny(s: String, duration: FiniteDuration) =
    assert(duration < tinyDuration, s"Assertion failed $s: $duration < $tinyDuration")

  def assertInitiating(s: String, duration: FiniteDuration) = {
    assert(
      duration < (windowDuration + tinyDuration),
      s"Assertion failed $s: $duration < ${windowDuration + tinyDuration}")
  }

  def assertWaitWindow(
      s: String,
      duration: FiniteDuration,
      tiny: FiniteDuration = tinyDuration) = {
    assert(
      duration > (windowDuration - tiny),
      s"Assertion failed $s: $duration > ${windowDuration - tiny}")
    assert(
      duration < (windowDuration + tiny),
      s"Assertion failed $s: $duration < ${windowDuration + tiny}")
  }

}
