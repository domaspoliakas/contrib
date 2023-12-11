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

import Function.const
import cats.effect.Concurrent
import cats.effect.Deferred
import cats.effect.Outcome
import cats.effect.Resource
import cats.effect.std.MapRef
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.Flags
import fs2.io.file.Path
import org.http4s.Response

object LocalCache {

  object ByString {
    def apply[F[_]: Concurrent](
        cached: Stream[F, Byte] => F[Stream[F, Byte]]): F[Cache[F, String, Response[F]]] =
      MapRef.ofSingleImmutableMap[F, String, State[F]]().map(new ByString(cached, _))
  }

  def localFileSystem[F[_]: Concurrent](
      fs: Files[F],
      rootDir: Path): Resource[F, Cache[F, String, Response[F]]] =
    fs.tempDirectory(Some(rootDir), "rs-cache-", None).evalMap { dir =>
      def cached(in: Stream[F, Byte]): F[Stream[F, Byte]] =
        fs.createTempFile(Some(dir), prefix = "", suffix = "", permissions = None)
          .flatMap(f => in.through(fs.writeAll(f)).compile.drain.as(f))
          .map(fs.readAll(_, 1024 * 1024, Flags.Read))

      ByString(cached)
    }

  ////

  private sealed abstract class State[F[_]]
  private final case class Writing[F[_]](
      result: Deferred[F, Outcome[F, Throwable, Response[F]]])
      extends State[F]
  private final case class Available[F[_]](result: F[Response[F]]) extends State[F]

  private final class ByString[F[_]](
      cached: Stream[F, Byte] => F[Stream[F, Byte]],
      mapRef: MapRef[F, String, Option[State[F]]])(implicit F: Concurrent[F])
      extends Cache[F, String, Response[F]] {

    private def populateCache(key: String, req: Resource[F, Response[F]]): F[Response[F]] =
      req.use(res => cached(res.body).map(res.withBodyStream)).guaranteeCase { oc =>
        mapRef(key).modify {
          case Some(Writing(result)) =>
            val next = oc.fold[Option[State[F]]](None, const(None), fa => Some(Available(fa)))
            (next, result.complete(oc).void)

          case other =>
            (
              other,
              F.raiseError[Unit](new IllegalStateException(
                s"Expected cache state for '$key' to be 'Writing', instead was $other")))
        }.flatten
      }

    def apply(key: String, in: Resource[F, Response[F]], force: Boolean): F[Response[F]] =
      F.deferred[Outcome[F, Throwable, Response[F]]].flatMap { d =>
        F.uncancelable { poll =>
          mapRef(key).modify {
            case s @ Some(Writing(result)) =>
              // await cache population and, if canceled, reattempt
              (s, poll(result.get.flatMap(_.embed(apply(key, in, force)))))

            case s @ Some(Available(result)) if !force =>
              (s, result)

            case _ =>
              (Some(Writing(d)), poll(populateCache(key, in)))
          }.flatten
        }
      }
  }
}
