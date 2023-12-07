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

package precog.contrib.http4s.logger

import java.nio.charset.StandardCharsets

import cats.effect.Concurrent
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all._
import fs2.Chunk
import fs2.Pipe
import fs2.Pull
import fs2.Stream
import org.http4s.Request
import org.http4s.Response
import org.http4s.Status
import org.http4s.client.Client
import org.typelevel.log4cats.Logger

object LoggerMiddleware {
  val DefaultMaxChunks = 10L

  def client[F[_]: Concurrent](logger: Logger[F]): Client[F] => Client[F] =
    complete[F](
      logger,
      logger.trace(_),
      { (status, text) =>
        status.responseClass match {
          case Status.Informational | Status.Successful | Status.Redirection =>
            logger.trace(text)
          case Status.ClientError | Status.ServerError => logger.warn(text)
        }
      }
    )

  def realtimeResponseChunks[F[_]: Concurrent](
      logger: Logger[F],
      max: Long = DefaultMaxChunks): Client[F] => Client[F] =
    realtimeResponseChunksLogger[F](
      max,
      logger.trace(_),
      { (status, text) =>
        status.responseClass match {
          case Status.Informational | Status.Successful | Status.Redirection =>
            logger.trace(text)
          case Status.ClientError | Status.ServerError => logger.warn(text)
        }
      }
    )

  def complete[F[_]: Concurrent](
      logger: Logger[F],
      logReq: String => F[Unit],
      logResp: (Status, String) => F[Unit]): Client[F] => Client[F] = { client =>
    def logResponse(resp: Response[F]): F[Unit] =
      resp.body.through(fs2.text.utf8.decode).compile.string.flatMap { str =>
        logResp(resp.status, bodyMessageWrap(resp, str))
      }

    def logRequest(req: Request[F]): F[Unit] =
      req.body.through(fs2.text.utf8.decode).compile.string flatMap { str =>
        logReq(reqMessageWrap(req, str))
      }

    responseLoggerComplete(logger, logResponse).apply(
      requestLoggerComplete(logRequest).apply(client))
  }

  def requestLoggerComplete[F[_]](logMessage: Request[F] => F[Unit])(
      implicit F: Concurrent[F]): Client[F] => Client[F] = { client =>
    Client { req =>
      Resource.suspend {
        Ref[F].of(Vector.empty[Chunk[Byte]]).flatMap { vec =>
          val collect: Pipe[F, Byte, Unit] =
            _.chunks.flatMap { s => Stream.exec(vec.update(_ :+ s)) }

          for {
            _ <- req.body.through(collect).compile.drain
            chunks <- vec.get
            newReq = req.withBodyStream(Stream.emits(chunks).unchunks)
            _ <- logMessage(newReq)
          } yield client.run(newReq)
        }
      }
    }
  }

  def responseLoggerComplete[F[_]](logger: Logger[F], logMessage: Response[F] => F[Unit])(
      implicit F: Concurrent[F]): Client[F] => Client[F] = { client =>
    def logResponse(response: Response[F]): Resource[F, Response[F]] =
      Resource.suspend {
        Ref[F].of(Vector.empty[Chunk[Byte]]).map { vec =>
          val dumpChunksToVec: Pipe[F, Byte, Nothing] =
            _.chunks.flatMap(s => Stream.exec(vec.update(_ :+ s)))

          Resource.make(
            // Cannot Be Done Asynchronously - Otherwise All Chunks May Not Be Appended before Finalization
            F.pure(response.withBodyStream(response.body.observe(dumpChunksToVec)))
          ) { r =>
            val newBody = Stream.eval(vec.get).flatMap(Stream.emits).unchunks
            logMessage(r.withBodyStream(newBody)).handleErrorWith(t =>
              logger.error(t)("Error logging response body"))
          }
        }
      }

    Client(req => client.run(req).flatMap(logResponse))
  }

  def realtimeResponseChunksLogger[F[_]: Concurrent](
      max: Long,
      logReq: String => F[Unit],
      logResp: (Status, String) => F[Unit]): Client[F] => Client[F] = { client =>
    def logRequest(req: Request[F]): F[Unit] =
      req.body.through(fs2.text.utf8.decode).compile.string flatMap { str =>
        logReq(reqMessageWrap(req, str))
      }

    responseLoggerRealtimeChunks(max, logResp).apply(
      requestLoggerComplete(logRequest).apply(client))
  }

  def responseLoggerRealtimeChunks[F[_]](max: Long, logPart: (Status, String) => F[Unit])(
      implicit F: Concurrent[F]): Client[F] => Client[F] = { client =>
    Client { req =>
      client.run(req).flatMap { resp =>
        def logger(ix: Long, part: String): F[Unit] =
          logPart(resp.status, ixMessageWrap(ix, bodyMessageWrap(resp, part)))

        val logBody: Stream[F, Byte] => Resource[F, Stream[F, Byte]] =
          logFirstN(max, logger)

        logBody(resp.body).map(body => resp.withBodyStream(body))

      }
    }
  }

  private def logFirstN[F[_]: Concurrent](
      max: Long,
      log: (Long, String) => F[Unit]): Stream[F, Byte] => Resource[F, Stream[F, Byte]] = {
    def logChunk(ix: Long, bytes: Chunk[Byte]) =
      log(ix, new String(bytes.toArray, StandardCharsets.UTF_8))

    logFirstNChunk(max, logChunk)
  }

  private def logFirstNChunk[F[_]: Concurrent](
      max: Long,
      logChunk: (Long, Chunk[Byte]) => F[Unit])
      : Stream[F, Byte] => Resource[F, Stream[F, Byte]] = { s =>
    logFirstNChunk0(Vector.empty, s, max, logChunk).stream.compile.resource.lastOrError
  }

  private def logFirstNChunk0[F[_]](
      init: Vector[Chunk[Byte]],
      s: Stream[F, Byte],
      max: Long,
      logChunk: (Long, Chunk[Byte]) => F[Unit]
  ): Pull[F, Stream[F, Byte], Unit] =
    s.pull.uncons.flatMap {
      case None =>
        if (init.isEmpty)
          Pull.eval(logChunk(0, Chunk.empty))
        else
          Pull.done
      case Some((chunk, remaining)) =>
        val size = init.size.toLong

        if (size < max)
          Pull
            .eval(logChunk(size, chunk))
            .flatMap(_ => logFirstNChunk0(init :+ chunk, remaining, max, logChunk))
        else
          Pull.output1(Stream.iterable(init :+ chunk).flatMap(Stream.chunk) ++ remaining)

    }

  private def bodyMessageWrap[F[_]](resp: Response[F], inp: String): String = {
    val prelude = s"${resp.httpVersion} ${resp.status}"
    val headers = resp.headers.headers.mkString("Headers(", ", ", ")")
    val body = s"""body="$inp""""
    s"$prelude${spaced(headers)}${spaced(body)}"
  }

  private def reqMessageWrap[F[_]](req: Request[F], inp: String): String = {
    val prelude = s"${req.httpVersion} ${req.uri}"
    val headers = req.headers.headers.mkString("Headers(", ", ", ")")
    val body = s"""body="$inp""""
    s"$prelude${spaced(headers)}${spaced(body)}"
  }

  private def ixMessageWrap(ix: Long, inp: String): String = {
    s"chunk-index=$ix${spaced(inp)}"
  }

  private def spaced(x: String): String = if (x.isEmpty) x else s" $x"
}
