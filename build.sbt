ThisBuild / scalaVersion := "2.13.8"

ThisBuild / githubRepository := "contrib"

ThisBuild / homepage := Some(url("https://github.com/precog/contrib"))

ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/precog/contrib"), "scm:git@github.com:precog/contrib.git"))

val CatsEffectVersion = "3.3.12"
val Http4sVersion = "0.23.12"
val Fs2Version = "3.2.3"
val Log4CatsVersion = "2.3.1"

lazy val root =
  project.in(file(".")).settings(noPublishSettings).aggregate(rateLimit, http4sLogger)

lazy val rateLimit = project
  .in(file("modules/rate-limit"))
  .settings(name := "contrib-ratelimit")
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % CatsEffectVersion
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
