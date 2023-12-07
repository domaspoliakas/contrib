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

import cats.effect.IO
import cats.effect.Resource
import cats.effect.kernel.Ref
import fs2.Stream
import org.http4s.Request
import org.http4s.Response
import org.http4s.Status
import org.http4s.client.Client

class LoggerSuite extends munit.CatsEffectSuite {

  test("Consumption of successfull and small response") {

    val stream: Stream[IO, Byte] = Stream.iterable("123".getBytes())

    val client: Client[IO] = Client(_ => Resource.pure(Response().withBodyStream(stream)))

    val expected = List("200 OK chunk-index=0 HTTP/1.1 200 OK Headers() body=\"123\"")

    for {
      ref <- Ref.of[IO, List[String]](List())
      f: ((Status, String) => IO[Unit]) = (status, str) =>
        ref.getAndUpdate(ls => ls :+ s"$status $str").void
      middleware = LoggerMiddleware.responseLoggerRealtimeChunks(10, f)

      _ <- middleware(client).run(Request[IO]()).use_
      ls <- ref.get
    } yield assertEquals(expected, ls)

  }

  test("Consumption of successfull response with N-Chunks preserving the remaining") {

    val bodyList = List("123", "456", "789")

    val stream: Stream[IO, Byte] =
      Stream.iterable(bodyList.map(_.getBytes())).flatMap(Stream.iterable(_))

    val client: Client[IO] = Client(_ => Resource.pure(Response().withBodyStream(stream)))

    val expected = List(
      "200 OK chunk-index=0 HTTP/1.1 200 OK Headers() body=\"123\"",
      "200 OK chunk-index=1 HTTP/1.1 200 OK Headers() body=\"456\""
    )

    for {
      ref <- Ref.of[IO, List[String]](List())
      f: ((Status, String) => IO[Unit]) = (status, str) =>
        ref.getAndUpdate(ls => ls :+ s"$status $str").void
      middleware = LoggerMiddleware.responseLoggerRealtimeChunks(2, f)
      body <- middleware(client)
        .run(Request[IO]())
        .use(r => r.body.chunks.map(_.toList.toArray).compile.toList)
        .map(bl => bl.map(l => String.valueOf(l.map(_.toChar))))
      ls <- ref.get
    } yield assertEquals((expected, bodyList), (ls, body))

  }

  test("Consumption of successfull empty response") {

    val client: Client[IO] = Client(_ => Resource.pure(Response().withEmptyBody))

    val expected = List("200 OK chunk-index=0 HTTP/1.1 200 OK Headers() body=\"\"")

    for {
      ref <- Ref.of[IO, List[String]](List())
      f: ((Status, String) => IO[Unit]) = (status, str) =>
        ref.getAndUpdate(ls => ls :+ s"$status $str").void
      middleware = LoggerMiddleware.responseLoggerRealtimeChunks(10, f)
      _ <- middleware(client).run(Request[IO]()).use_
      ls <- ref.get
    } yield assertEquals(expected, ls)

  }

}
