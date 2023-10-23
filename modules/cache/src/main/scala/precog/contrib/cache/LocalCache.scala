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

import java.util.UUID
import java.util.concurrent.CancellationException

import cats.effect.Concurrent
import cats.effect.Resource
import cats.effect.kernel.Deferred
import cats.effect.std.MapRef
import cats.effect.std.UUIDGen
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.Flags
import fs2.io.file.Path
import org.http4s.Response

object LocalCache {
  type SB[F[_]] = Response[F]
  private type ESB[F[_]] = Either[Throwable, SB[F]]
  private type DSB[F[_]] = Deferred[F, Either[Throwable, SB[F]]]
  private type MR[F[_]] = MapRef[F, String, Option[State[F]]]

  private sealed abstract class State[F[_]]
  private final case class Writing[F[_]](uuid: UUID, result: DSB[F]) extends State[F]
  private final case class Available[F[_]](result: SB[F]) extends State[F]

  private final class ByString[F[_]: UUIDGen](
      cached: Stream[F, Byte] => F[Stream[F, Byte]],
      mapRef: MR[F])(implicit F: Concurrent[F])
      extends Cache[F, String, SB[F]] {

    private def populateCache(
        uuid: UUID,
        req: Resource[F, SB[F]],
        d: DSB[F],
        key: String): F[Unit] =
      req
        .use(res => cached(res.body).map(b => res.copy(body = b)))
        .guaranteeCase { oc =>
          val result =
            oc.fold[F[ESB[F]]](
              F.pure(
                Left(
                  new CancellationException(s"Cache population for key '$key' was canceled."))),
              err => F.pure(Left(err)),
              _.map(Right(_))
            )

          F.uncancelable { _ =>
            result.flatMap { result =>
              mapRef(key).update {
                case Some(Writing(stateUUID, _)) if stateUUID == uuid =>
                  result match {
                    // We don't store failed results
                    case Left(_) => None
                    case Right(v) => Some(Available(v))
                  }

                case other =>
                  other

              } >> d.complete(result).void
            }
          }
        }
        .void

    def apply(key: String, in: Resource[F, SB[F]], force: Boolean): F[SB[F]] =
      (F.deferred[ESB[F]], UUIDGen[F].randomUUID).flatMapN { (newD, uuid) =>
        F.uncancelable { poll =>
          mapRef(key).modify {
            case s @ Some(Writing(_, result)) =>
              (s, poll(result.get.rethrow))
            case s @ Some(Available(result)) if !force =>
              (s, F.pure(result))

            case _ =>
              val s = Some(Writing(uuid, newD))
              (
                s,
                // we background to be able to handle the case when population self-cancels
                poll(populateCache(uuid, in, newD, key).background.surround(newD.get.rethrow)))
          }.flatten
        }
      }
  }

  object ByString {
    def apply[F[_]: Concurrent: UUIDGen](
        cached: Stream[F, Byte] => F[Stream[F, Byte]]): F[Cache[F, String, Response[F]]] =
      MapRef.ofSingleImmutableMap[F, String, State[F]]().map(new ByString(cached, _))
  }

  def localFileSystem[F[_]: Concurrent: UUIDGen](
      fs: Files[F],
      rootDir: Path): Resource[F, Cache[F, String, Response[F]]] =
    fs.tempDirectory(Some(rootDir), "rs-cache-", None).evalMap { dir =>
      def cached(in: Stream[F, Byte]): F[Stream[F, Byte]] =
        fs.createTempFile(Some(dir), prefix = "", suffix = "", permissions = None)
          .flatMap(f => in.through(fs.writeAll(f)).compile.drain.as(f))
          .map(fs.readAll(_, 1024 * 1024, Flags.Read))

      ByString(cached)
    }
}
