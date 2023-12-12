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

import cats.effect.IO
import cats.effect.Resource
import cats.kernel.BoundedEnumerable
import cats.syntax.all._
import fs2.Chunk
import fs2.Stream
import munit.CatsEffectSuite
import org.http4s.Response

final class LocalCacheSuite extends CatsEffectSuite {

  val cache =
    LocalCache.ByString[IO](_.compile.to(Chunk).map(Stream.chunk))

  val responses: IO[Resource[IO, Response[IO]]] =
    IO.ref('a').map { ctr =>
      Resource.eval(ctr.getAndUpdate(BoundedEnumerable[Char].cycleNext)).map { c =>
        Response(body = Stream.emit(c.toByte))
      }
    }

  test("cache response") {
    (cache, responses).flatMapN { (c, r) =>
      c("k", r, false)
        .parReplicateA(4)
        .flatMap(_.traverse(_.bodyText.compile.foldMonoid))
        .assertEquals(List("a", "a", "a", "a"))
    }
  }

  test("reacquire cached response when forced") {
    (cache, responses).flatMapN { (c, r) =>
      (c("k", r, false) >> c("k", r, true))
        .flatMap(_.bodyText.compile.foldMonoid)
        .assertEquals("b")
    }
  }

  test("forcing doesn't reattempt inflight response") {
    fail("todo")
  }

  test("propagate errors during cache population") {
    fail("todo")
  }

  test("translate response self-cancelation into error") {
    fail("todo")
  }

  test("translate external population cancelation into error") {
    fail("todo")
  }
}
