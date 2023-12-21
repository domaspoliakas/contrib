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
import cats.effect.kernel.Ref
import cats.implicits._
import munit.CatsEffectSuite

class LimiterSuite extends CatsEffectSuite {

  test("Limiter submit request succesfully") {
    val lf: LimitFunction[IO, Unit] = new LimitFunction[IO, Unit] {

      def request: IO[Either[Instant, IO[Unit]]] =
        IO(IO.unit.asRight)

      def update(a: Unit): IO[Unit] = IO.unit

    }

    Limiter
      .limiter[IO, Unit](lf, 1, 1)
      .evalMap(l => Ref.of[IO, Int](0).map(ref => (l, ref)))
      .use {
        case (limiter, ref) =>
          val task = ref.update(_ + 1)

          for {
            _ <- limiter.submit(task)
            res <- ref.get
          } yield assert(res == 1)

      }

  }

  test("Limiter cancels request succesfully") {
    val lf: LimitFunction[IO, Unit] = new LimitFunction[IO, Unit] {

      def request: IO[Either[Instant, IO[Unit]]] =
        IO(IO.unit.asRight)

      def update(a: Unit): IO[Unit] = IO.unit

    }

    Limiter
      .limiter[IO, Unit](lf, 1, 1)
      .evalMap(l => Ref.of[IO, Int](0).map(ref => (l, ref)))
      .use {
        case (limiter, ref) =>
          val task = (IO.sleep(5.seconds) *> ref.update(_ + 1)).onCancel(ref.update(_ - 1).void)

          for {
            f1 <- limiter.submit(task).start
            _ <- IO.sleep(1.seconds)
            _ <- f1.cancel
            res <- ref.get
          } yield assert(res == -1)

      }

  }

  test("Limiter handle multiple submits") {
    val lf: LimitFunction[IO, Unit] = new LimitFunction[IO, Unit] {

      def request: IO[Either[Instant, IO[Unit]]] =
        IO(IO.unit.asRight)

      def update(a: Unit): IO[Unit] = IO.unit

    }

    Limiter
      .limiter[IO, Unit](lf, 1, 1)
      .evalMap(l => Ref.of[IO, String]("Nothing").map(ref => (l, ref)))
      .use {
        case (limiter, ref) =>
          val task = IO.sleep(3.seconds) *> ref.set("First")

          val task2 = IO.sleep(1.seconds) *> ref.set("Second")

          for {
            f1 <- limiter.submit(task).start
            f2 <- limiter.submit(task2).start
            r1 <- f1.join *> ref.get
            r2 <- f2.join *> ref.get
          } yield assert((r1, r2) == ("First", "Second"))

      }

  }

  test("Limiter respect FIFO semantics") {
    val lf: LimitFunction[IO, Unit] = new LimitFunction[IO, Unit] {

      def request: IO[Either[Instant, IO[Unit]]] =
        IO(IO.unit.asRight)

      def update(a: Unit): IO[Unit] = IO.unit

    }

    Limiter
      .limiter[IO, Unit](lf, 1, 1)
      .evalMap(l => Ref.of[IO, (Long, Long)]((0, 0)).map(ref => (l, ref)))
      .use {
        case (limiter, ref) =>
          val task = IO.sleep(5.seconds) *> IO
            .realTimeInstant
            .flatMap(now => ref.getAndUpdate(v => (now.toEpochMilli, v._2)))
            .void

          val task2 = IO
            .realTimeInstant
            .flatMap(now => ref.getAndUpdate(v => (v._1, now.toEpochMilli)))
            .void

          for {
            _ <- limiter.submit(task).start
            f2 <- limiter.submit(task2).start
            _ <- f2.join
            r <- ref.get
          } yield assert(r._2 > r._1 && r._1 != 0 && r._2 != 0)

      }

  }

  test("Cancel respect queue semantics") {
    val lf: LimitFunction[IO, Unit] = new LimitFunction[IO, Unit] {

      def request: IO[Either[Instant, IO[Unit]]] =
        IO(IO.unit.asRight)

      def update(a: Unit): IO[Unit] = IO.unit

    }

    Limiter
      .limiter[IO, Unit](lf, 1, 1)
      .evalMap(l => Ref.of[IO, (Long, Long, Long)]((0, 0, 0)).map(ref => (l, ref)))
      .use {
        case (limiter, ref) =>
          val task = IO
            .sleep(2.seconds)
            .flatMap(_ =>
              IO.realTimeInstant
                .flatMap(now => ref.update(v => (now.toEpochMilli, v._2, v._3)))
                .void)
            .onCancel(ref.update(v => (-1, v._2, v._3)).void)

          val task2 = IO.sleep(1.seconds) *> IO
            .realTimeInstant
            .flatMap(now => ref.getAndUpdate(v => (v._1, now.toEpochMilli, v._3)))
            .void

          val task3 = IO
            .realTimeInstant
            .flatMap(now => ref.getAndUpdate(v => (v._1, v._2, now.toEpochMilli)))
            .void

          for {
            f1 <- limiter.submit(task).start
            _ <- IO.sleep(100.milli)
            f2 <- limiter.submit(task2).start
            _ <- IO.sleep(100.milli)
            f3 <- limiter.submit(task3).start
            _ <- f1.cancel
            _ <- f2.join
            _ <- f3.join
            r <- ref.get
          } yield assert(r._3 > r._2 && r._1 == -1 && r._3 != 0 && r._2 != 0)

      }

  }

}
