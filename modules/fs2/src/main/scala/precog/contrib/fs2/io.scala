/*
 * Copyright 2021 Precog Data
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

package precog.contrib.fs2

import cats.effect.Sync
import cats.implicits._
import fs2.Stream

object io {

  def resourceAsBytes[F[_]: Sync](
      resourceName: String,
      cl: ClassLoader = getClass.getClassLoader,
      chunkSize: Int = 32 * 1024): Stream[F, Byte] =
    fs2
      .io
      .readInputStream(
        Sync[F].defer(
          Option(cl.getResourceAsStream(resourceName))
            .liftTo[F](new Exception(s"Resource not found: $resourceName"))),
        chunkSize)
}
