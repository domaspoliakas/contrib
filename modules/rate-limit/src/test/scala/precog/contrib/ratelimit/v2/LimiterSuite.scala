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

package precog.contrib.ratelimit.v2

import java.time.Instant
import scala.concurrent.duration._

import cats.effect.IO
import cats.implicits._
import munit.CatsEffectSuite

class LimiterSuite extends CatsEffectSuite {

  test("supervisor test") {

    val neverLf: LimitFunction[IO, Unit] = new LimitFunction[IO, Unit] {

      def request: IO[Either[Instant, IO[Unit]]] =
        IO.realTimeInstant.map(_.plusSeconds(1).asLeft)

      def update(a: Unit): IO[Unit] = IO.unit

    }

    Limiter.limiter[IO, Unit](neverLf, 1, 1).use { limiter =>
      val task =
        IO.println("Task Can started")
          .flatMap(_ => IO.sleep(10.seconds) *> IO.println("Task Can finished"))
          .onCancel(IO.println("Task Can was cancelled"))

      val taskUnc = IO
        .println("Task Unc Started")
        .flatMap(_ => IO.sleep(10.seconds) *> IO.println("Task Unc finished"))

      for {
        f1 <- limiter.submit(task).start
        f2 <- limiter.submit(taskUnc).start

        _ <- IO.sleep(3.seconds)
        _ <- IO.println("About to cancel")
        _ <- f1.cancel
        _ <- f2.join
        _ <- IO.sleep(3.seconds)
      } yield ()

    }

  }

  /*
  test("to be removed ") {

    for {
      q <- Queue.unbounded[IO, IO[Int]]
      fb <- IO.sleep(3.seconds).as(2).start
      _ <- q.offer(IO.sleep(1.seconds).as(1))
      _ <- q.offer(fb.join.flatMap(_.embed(IO(0))))
      _ <- q.offer(IO.sleep(3.seconds).as(3))
      _ <- fb.cancel
      str = Stream.fromQueueUnterminated(q, 2)
      f <- str.evalMap(fi => fi.flatMap(i => IO.println(s"Stream $i"))).compile.drain.start
      _ <- IO.sleep(1.seconds)
      _ <- q.size.flatMap(s => IO.println(s"After sleep $s"))
      _ <- q.offer(IO.sleep(1.seconds).as(4))
      _ <- q.offer(IO.sleep(1.seconds).as(4))

      _ <- f.join

    } yield assert(true)

  }
   */

}
