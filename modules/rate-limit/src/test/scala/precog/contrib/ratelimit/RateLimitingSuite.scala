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

import cats.effect.IO
import cats.effect.Resource
import cats.effect.testkit.TestControl
import precog.contrib.ratelimit.LocalRateLimiting
import precog.contrib.ratelimit.RateLimiting
import precog.contrib.ratelimit.RedisTestkit

class LocalRateLimitingSuite
    extends RateLimitingSuite(LocalRateLimiting[IO], TestControl.executeEmbed(_))

class RedisRateLimitingSuite extends RateLimitingSuite(RedisTestkit.rateLimiting, identity) {

  test("redis: recovers from network failure") {
    val rl = RedisTestkit.flakyRateLimiting

    rl.rateLimit("flaky-id", 2, windowDuration, RateLimiting.Mode.Counting).flatMap { signals =>
      def until6(i: Int): IO[Unit] =
        if (i < 6)
          signals.limit >> until6(i + 1)
        else
          IO.unit

      (sleepToBeginWindow >> IO.realTime).flatMap { started =>
        (until6(0) >> IO.realTime).flatMap { ended =>
          val elapsed = ended - started
          // 2 because it will finish at the start of the 3rd window
          val lowerBound = windowDuration * 2 - tinyDuration // epsilon
          IO(assert(lowerBound <= elapsed, s"expected ${lowerBound} <= ${elapsed}"))
        }
      }
    }
  }
}

abstract class RateLimitingSuite(
    rateLimiting: Resource[IO, RateLimiting[IO]],
    testControl: IO[Unit] => IO[Unit])
    extends munit.CatsEffectSuite {

  val tinyDuration = 100.millis
  val windowDuration = 2.seconds

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

  test("counting: supports windows shorter than 1 second") {
    testControl {
      rateLimiting.use { rl =>
        val subSecond = 500.millis

        for {
          signals <- rl.rateLimit("id", 2, 500.millis, RateLimiting.Mode.Counting)
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
          assertInitiating("nowInit1 - nowInit0", nowInit1 - nowInit0, window = subSecond)
          assertInitiating("now0 - nowInit1", now0 - nowInit1, window = subSecond)
          assertTiny("now1 - now0", now1 - now0)
          assertWaitWindow("now2 - now1", now2 - now1, window = subSecond)
          assertTiny("now3 - now2", now3 - now2)
          assertWaitWindow("now4 - now3", now4 - now3, window = subSecond)
          assertTiny("now5 - now4", now5 - now4)
          assertWaitWindow("now6 - now5", now6 - now5, window = subSecond)
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

  def sleepToBeginWindow =
    IO.realTime
      .flatMap(now => IO.sleep(stableEnd(now, windowDuration) - now + (tinyDuration / 5)))

  private def stableEnd(now: FiniteDuration, window: FiniteDuration): FiniteDuration =
    (((now.toSeconds / window.toSeconds) + 1) * window.toSeconds).seconds

  def assertTiny(s: String, duration: FiniteDuration) =
    assert(duration < tinyDuration, s"Assertion failed $s: $duration < $tinyDuration")

  def assertInitiating(
      s: String,
      duration: FiniteDuration,
      window: FiniteDuration = windowDuration) = {
    assert(
      duration < (window + tinyDuration),
      s"Assertion failed $s: $duration < ${window + tinyDuration}")
  }

  def assertWaitWindow(
      s: String,
      duration: FiniteDuration,
      tiny: FiniteDuration = tinyDuration,
      window: FiniteDuration = windowDuration) = {
    assert(duration > (window - tiny), s"Assertion failed $s: $duration > ${window - tiny}")
    assert(
      duration < (windowDuration + tiny),
      s"Assertion failed $s: $duration < ${window + tiny}")
  }

}
