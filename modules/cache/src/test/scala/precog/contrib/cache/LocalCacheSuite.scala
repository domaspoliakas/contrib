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
import cats.syntax.all._
import fs2.Chunk
import fs2.Stream
import munit.CatsEffectSuite
import org.http4s.Response

final class LocalCacheSuite extends CatsEffectSuite {

  val cache =
    LocalCache.ByString[IO](_.compile.to(Chunk).map(Stream.chunk))

  test("immediately canceling cached value doesn't deadlock") {
    cache.flatMap { c =>
      val n = 1024
      val go = c("foo", Resource.pure(Response[IO]()), false)
      val many = List.fill(n)(go)

      many
        .map(_.start.flatMap(fib => IO.cede >> fib.cancel))
        .parSequence
        .productR(many.parSequence)
        .map(_.length)
        .assertEquals(n)
    }
  }
}
