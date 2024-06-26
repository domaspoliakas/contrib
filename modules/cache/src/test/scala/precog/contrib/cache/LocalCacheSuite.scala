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

package precog.contrib.cache
import java.util.concurrent.CancellationException
import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.Resource
import cats.effect.testkit.TestControl
import cats.kernel.BoundedEnumerable
import cats.syntax.all._
import fs2.Chunk
import fs2.Stream
import munit.CatsEffectSuite
import org.http4s.Response
import org.http4s.Status

final class LocalCacheSuite extends CatsEffectSuite {

  val cache: IO[Cache[IO, String, Response[IO]]] =
    LocalCache.ByString[IO](_.compile.to(Chunk).map(Stream.chunk))

  val responses: IO[Resource[IO, Response[IO]]] =
    IO.ref('a').map { ctr =>
      Resource.eval(ctr.getAndUpdate(BoundedEnumerable[Char].cycleNext)).map { c =>
        Response(body = Stream.emit(c.toByte))
      }
    }

  test("cache response") {
    TestControl.executeEmbed((cache, responses).flatMapN { (c, r) =>
      c("k", r, None)
        .parReplicateA(4)
        .flatMap(_.traverse(_.bodyText.compile.foldMonoid))
        .assertEquals(List("a", "a", "a", "a"))
    })
  }

  test("reacquire cached response when forced") {
    TestControl.executeEmbed((cache, responses).flatMapN { (c, r) =>
      c("k", r, None)
        .flatMap { r1 => c("k", r, r1.some) }
        .flatMap(_.bodyText.compile.foldMonoid)
        .assertEquals("b")
    })
  }

  test("forcing doesn't reattempt inflight response") {

    TestControl.executeEmbed(
      (cache, responses, IO.deferred[Unit]).flatMapN { (c, r, started) =>
        val first =
          c("k", Resource.eval(started.complete(()) >> IO.sleep(250.millis)) >> r, None)

        val second =
          started.get >> c(
            "k",
            r,
            Response[IO](body = Stream.emit('a'.toByte)).some
          )

        first
          .background
          .use(f => second <* f)
          .flatMap(_.bodyText.compile.foldMonoid)
          .assertEquals("a")
      }
    )
  }

  test("propagate errors during cache population") {
    TestControl.executeEmbed(
      cache.flatMap { c =>
        c(
          "k",
          Resource.raiseError[IO, Response[IO], Throwable](CacheSuiteException),
          None
        ).intercept[CacheSuiteException.type]

      }
    )
  }

  test("translate response self-cancelation into error") {
    TestControl.executeEmbed(
      cache.flatMap { c =>
        c(
          "k",
          Resource.eval(IO.canceled.as(null: Response[IO])),
          None
        ).intercept[CancellationException]
      }
    )
  }

  test("translate external population cancelation into error".ignore) {
    fail("todo")
  }

  test("external cancelation should gracefully release the cache key for later use") {
    TestControl.executeEmbed {
      val test = (cache, responses).flatMapN { (c, r) =>
        val first = c("k", Resource.eval(IO.never), None)
        val second = c("k", r, None)

        (first.start.flatMap(_.cancel) >> second)
          .flatMap(_.bodyText.compile.foldMonoid)
          .assertEquals("a")
      }

      // Race conditions are hard, best try this a few times to make sure we
      // catch problems
      test.replicateA_(10000)
    }
  }

  test("Versioning overrides when hits  `cache == currentValue` (by X-Precog-Token) ") {

    TestControl.executeEmbed {

      cache.flatMap { c =>
        def mkReq(newStatus: Status, expected: Status, cached: Option[Response[IO]]) =
          c.apply("key", Resource.pure(Response(newStatus)), cached)
            .flatMap(r => IO(assert(r.status == expected)).as(r))

        val NoCache = None

        for {
          f <- mkReq(
            newStatus = Status.Ok,
            expected = Status.Ok,
            cached = NoCache
          )
          _ <- mkReq(
            newStatus = Status.Accepted,
            expected = Status.Accepted,
            cached = f.some
          )

          t <- mkReq(
            newStatus = Status.InternalServerError,
            expected = Status.Accepted,
            cached = f.some // Older cache means we're not evaluating to InternalServerError
          )
          _ <- mkReq(
            newStatus = Status.Created,
            expected = Status.Created,
            cached = t.some
          )
        } yield ()

      }

    }

  }

  //

  private case object CacheSuiteException extends Exception
}
