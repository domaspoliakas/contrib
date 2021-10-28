ThisBuild / scalaVersion := "2.13.6"

ThisBuild / githubRepository := "contrib"

ThisBuild / homepage := Some(url("https://github.com/precog/contrib"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/contrib"),
  "scm:git@github.com:precog/contrib.git"))

ThisBuild / publishAsOSSProject := false

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val precogVersion = Def.setting[String](managedVersions.value("precog-precog"))

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(rateLimit)

lazy val rateLimit = project
  .in(file("modules/rate-limit"))
  .settings(name := "rate-limit")
  .settings(
    libraryDependencies ++= Seq(
      "com.precog" %% "precog-spi" % precogVersion.value
    )
  )
