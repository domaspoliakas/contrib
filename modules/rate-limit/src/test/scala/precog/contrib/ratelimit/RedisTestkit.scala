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

package mongo4cats.testkit

import java.io.File

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.comcast.ip4s.Host
import com.comcast.ip4s.Port
import io.chrisdavenport.rediculous.RedisConnection

object RedisTestkit {

  def sharedContainer =
    sharedResource(
      com
        .dimafeng
        .testcontainers
        .DockerComposeContainer(
          com
            .dimafeng
            .testcontainers
            .DockerComposeContainer
            .ComposeFile(new File(
              getClass.getClassLoader.getResource("docker-compose.yml").getPath).asLeft),
          Seq(com.dimafeng.testcontainers.ExposedService("redis", 6379))
        ))

  def connection: Resource[IO, RedisConnection[IO]] =
    sharedContainer.flatMap { c =>
      RedisConnection
        .direct[IO]
        .withHost(
          Host
            .fromString(c.getServiceHost("redis", 6379))
            .getOrElse(sys.error("Could not get service host for redis")))
        .withPort(
          Port
            .fromInt(c.getServicePort("redis", 6379))
            .getOrElse(sys.error("Could not get service port for redis")))
        .build
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
