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

  test("versioning") {

    TestControl.executeEmbed {

      cache.flatMap { c =>
        val first = c.apply("key", Resource.pure(Response(Status.Ok)), None)
        val second = c.apply("key", Resource.pure(Response(Status.Accepted)), Some(Response(Status.Ok)))
        val third = c.apply("key", Resource.pure(Response(Status.InternalServerError)), Some(Response(Status.Ok)))
        val fourth = c.apply("key", Resource.pure(Response(Status.Created)), Some(Response(Status.Accepted)))

        first.map(_.status).assertEquals(Status.Ok) >>
          // If the version matches then we make the next request
          second.map(_.status).assertEquals(Status.Accepted) >>
          // If the version does not match then we _don't_ make the request
          third.map(_.status).assertEquals(Status.Accepted) >>
          // Then we make one more request for funsies
          fourth.map(_.status).assertEquals(Status.Created)

      }

    }

  }

  //

  private case object CacheSuiteException extends Exception
}
