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

import java.net.InetSocketAddress
import java.net.ServerSocket
import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.Resource
import cats.effect.std.Random
import cats.effect.syntax.all._
import cats.syntax.all._
import com.comcast.ip4s.Host
import com.comcast.ip4s.Port
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import com.dimafeng.testcontainers.SingleContainer
import io.chrisdavenport.rediculous.RedisConnection
import retry._

object RedisTestkit {

  val RedisPort: Int = 6379

  val freeEphemeralPort: IO[Int] =
    IO(new ServerSocket).bracket(ss =>
      IO.blocking(ss.bind(new InetSocketAddress(0))) >> IO(ss.getLocalPort))(ss =>
      IO.blocking(ss.close))

  val redisContainer: IO[SingleContainer[_]] =
    freeEphemeralPort.map { hostPort =>
      FixedHostPortGenericContainer(
        imageName = "redis:7.0.11",
        command = Seq("redis-server", "--appendonly", "yes", "--appendfsync", "always"),
        exposedContainerPort = RedisPort,
        exposedHostPort = hostPort
      )
    }

  def container: Resource[IO, SingleContainer[_]] =
    Resource.make(redisContainer.flatTap(c => IO.blocking(c.start())))(c =>
      IO.blocking(c.stop()))

  def containerConnection(c: SingleContainer[_]): Resource[IO, RedisConnection[IO]] = {
    val serviceHost = c.containerIpAddress
    val servicePort = c.mappedPort(RedisPort)

    RedisConnection
      .direct[IO]
      .withHost(
        Host
          .fromString(serviceHost)
          .getOrElse(sys.error(s"Could not create host from '$serviceHost'")))
      .withPort(
        Port
          .fromInt(servicePort)
          .getOrElse(sys.error(s"Could not create port from '$servicePort")))
      .build
  }

  def rateLimiting: Resource[IO, RateLimiting[IO]] =
    container.map(cont =>
      RedisRateLimiting[IO](
        containerConnection(cont),
        RetryPolicies.alwaysGiveUp,
        (_, _) => IO.unit))

  def flakify(c: SingleContainer[_]): Resource[IO, Unit] =
    Random.scalaUtilRandom[IO].toResource.flatMap { rnd =>
      val restart =
        IO.blocking(c.dockerClient.restartContainerCmd(c.containerId).exec())

      val disrupt =
        rnd
          .betweenInt(250, 1000)
          .flatMap { tout =>
            if ((tout % 20) > 16)
              //IO.println(s"RESTART ${c.containerId}") >>
              restart >>
                IO.sleep(tout.millis)
            else
              IO.sleep(tout.millis)
          }
          .foreverM

      (IO.sleep(500.millis) >> disrupt).background.void
    }

  def flakyConnection: Resource[IO, RedisConnection[IO]] =
    container.flatMap { c => flakify(c) >> containerConnection(c) }

  def flakyRateLimiting: RateLimiting[IO] =
    RedisRateLimiting[IO](
      flakyConnection,
      RetryPolicies.limitRetries[IO](10) join RetryPolicies.constantDelay(250.millis),
      (_, _) => IO.unit
      //(t: Throwable, d: RetryDetails) => IO.println(s"  RETRY $t $d")
    )
}
