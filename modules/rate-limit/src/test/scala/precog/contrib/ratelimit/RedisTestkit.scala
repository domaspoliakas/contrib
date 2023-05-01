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

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.comcast.ip4s.Host
import com.comcast.ip4s.Port
import com.dimafeng.testcontainers.GenericContainer
import io.chrisdavenport.rediculous.RedisConnection
import retry._

object RedisTestkit {

  val redisContainer = GenericContainer(
    dockerImage = "redis:7.0.10",
    exposedPorts = List(6379)
  )

  def container = Resource.make {
    val c = redisContainer
    IO.delay(c.start()).as(c)
  }(c => IO.delay(c.stop()))

  def containerConn(c: GenericContainer): RedisConnection.PooledConnectionBuilder[IO] = {
    val serviceHost = c.containerIpAddress
    val servicePort = c.mappedPort(6379)
    RedisConnection
      .pool[IO]
      .withHost(
        Host
          .fromString(serviceHost)
          .getOrElse(sys.error(s"Could not create host from '$serviceHost'")))
      .withPort(
        Port
          .fromInt(servicePort)
          .getOrElse(sys.error(s"Could not create port from '$servicePort")))
    // .build

  }

  def connection: Resource[IO, RedisConnection[IO]] =
    container.flatMap { c => containerConn(c).build }

  def flakify(c: GenericContainer): IO[Unit] =
    IO.sleep(FiniteDuration(300, TimeUnit.MILLISECONDS)) >> IO.println("stopping") >> IO.delay(
      c.stop()) >> IO.println("stopped") >>
      IO.sleep(FiniteDuration(300, TimeUnit.MILLISECONDS)) >> IO.println("starting") >> IO
        .delay(
          c.start()
        ) >> IO.println("started") // >> flakify(c)

  def flakyBuilder: Resource[IO, RedisConnection.PooledConnectionBuilder[IO]] = {
    container.flatMap { c =>
      val b = containerConn(c)
      Resource.eval(flakify(c)).as(b)
    // val conn0 = containerConn(c).flatMap { conn => flakify(c).background.as(conn) }
    // conn0
    // retryConn(conn0)
    }
  }

  def flaky(b: RedisConnection.PooledConnectionBuilder[IO])
      : Resource[IO, Ref[IO, RedisConnection[IO]]] = {
    b.build.flatMap(c => Resource.eval(Ref.of[IO, RedisConnection[IO]](c)))
    // flakyBuilder.flatMap { b =>
    //   b.build
    // }
  }

  def flakyRateLimiting: Resource[IO, RateLimiting[IO]] = {
    flakyBuilder.flatMap { builder =>
      println("builder")
      flaky(builder).map { ref =>
        println("ref")
        RedisRateLimiting[IO](
          IO.println("getting connection") >> ref.get,
          IO.println("refreshing connection") >>
            // We need to create another builder on refresh??
            // This seems weird, I'd expect pooled/queued to give access to some intermediate structure
            // so that we can take e.g. another conn from the pool
            containerConn(redisContainer)
              .build
              .flatMap(c => Resource.eval(ref.set(c)))
              .allocated
              .map(_._1),
          RetryPolicies.limitRetries(15),
          (t: Throwable, d: RetryDetails) => IO.println(s"rt $t $d")
        )
      }
    }
  }

  def onErr(s: Throwable, d: RetryDetails): X[Unit] =
    Resource.eval(IO.println(s"XXX Got error $d $s "))

  type X[A] = Resource[IO, A]

  def retryConn(conn: Resource[IO, RedisConnection[IO]]) = {
    val y: RetryPolicy[X] = RetryPolicies.limitRetries[X](5)
    val xx = retry.retryingOnAllErrors[RedisConnection[IO]](
      policy = y,
      onError = (s, d) => onErr(s, d)
    )(conn)
    xx
  }

  // //////

  // Normally we'd want to stop the container after the suite
  // However, we're using the same container in multiple suites
  // Following the workaround here we simply don't stop the container
  // https://github.com/testcontainers/testcontainers-scala/issues/160
  def sharedResource[C <: com.dimafeng.testcontainers.DockerComposeContainer](
      containerDefinition: C
  ): Resource[IO, C] = {
    val readyDeferred = Deferred.unsafe[IO, Unit]
    val refContainer = Ref.unsafe[IO, Option[C]](None)

    Resource.eval(
      refContainer
        .modify[(Boolean, C)] {
          case None => (Some(containerDefinition), (true, containerDefinition))
          case Some(existing) => (Some(existing), (false, existing))
        }
        .flatMap {
          case (true, c) => IO.delay(c.start()) >> readyDeferred.complete(()).as(c)
          case (false, c) => readyDeferred.get >> IO.pure(c)
        }
    )
  }

}
