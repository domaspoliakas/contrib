ThisBuild / scalaVersion := "2.13.8"

ThisBuild / githubRepository := "contrib"

ThisBuild / homepage := Some(url("https://github.com/precog/contrib"))

ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/precog/contrib"), "scm:git@github.com:precog/contrib.git"))

val CatsEffectVersion = "3.3.14"
val Http4sVersion = "0.23.14"
val Log4CatsVersion = "2.4.0"
val MunitCatsEffectVersion = "1.0.7"
val MunitVersion = "0.7.29"
val RediculousVersion = "0.5.0"
val TestContainersVersion = "0.40.9"

lazy val root =
  project.in(file(".")).settings(noPublishSettings).aggregate(rateLimit, http4sLogger)

lazy val rateLimit = project
  .in(file("modules/rate-limit"))
  .settings(name := "contrib-ratelimit")
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % CatsEffectVersion,
      "io.chrisdavenport" %% "rediculous" % RediculousVersion,
      "com.dimafeng" %% "testcontainers-scala-core" % TestContainersVersion % Test,
      "org.scalameta" %% "munit" % MunitVersion % Test,
      "org.typelevel" %% "cats-effect-testkit" % CatsEffectVersion % Test,
      "org.typelevel" %% "munit-cats-effect-3" % MunitCatsEffectVersion % Test
    )
  )

lazy val http4sLogger = project
  .in(file("modules/http4s-logger"))
  .settings(name := "contrib-logger")
  .settings(
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-client" % Http4sVersion,
      "org.typelevel" %% "log4cats-slf4j" % Log4CatsVersion
    )
  )
