ThisBuild / scalaVersion := "2.13.6"

ThisBuild / githubRepository := "contrib"

ThisBuild / homepage := Some(url("https://github.com/precog/contrib"))

ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/precog/contrib"), "scm:git@github.com:precog/contrib.git"))

ThisBuild / publishAsOSSProject := false

val CatsEffectVersion = "3.2.5"

lazy val root = project.in(file(".")).settings(noPublishSettings).aggregate(rateLimit)

lazy val rateLimit = project
  .in(file("modules/rate-limit"))
  .settings(name := "contrib-ratelimit")
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % CatsEffectVersion
    )
  )
