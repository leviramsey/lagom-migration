ThisBuild / organization := "com.example"
ThisBuild / version := "0.0.1"

ThisBuild / scalaVersion := "2.13.16"

ThisBuild / libraryDependencySchemes +=
  "org.scala-lang.modules" %% "scala-java8-compat" % VersionScheme.Always

val macwire = "com.softwaremill.macwire" %% "macros" % "2.3.3" % "provided"
val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1" % Test
val postgres = "org.postgresql" % "postgresql" % "42.7.5"

lazy val meetup =
  (project in file("."))
    .aggregate(`meetup-api`, `meetup-impl`)

lazy val `meetup-api` =
  (project in file("meetup-api"))
    .settings(
      libraryDependencies ++= Seq(
        lagomScaladslApi),
      scalaVersion := "2.13.16"
    )

lazy val `meetup-impl` =
  (project in file("meetup-impl"))
    .enablePlugins(LagomScala)
    .settings(
      libraryDependencies ++= Seq(
        lagomScaladslPersistenceJdbc,
        lagomScaladslKafkaBroker,
        lagomScaladslTestKit,
        macwire,
        scalaTest,
        postgres),
      scalaVersion := "2.13.16")
    .settings(lagomForkedTestSettings)
    .settings(
      assemblyMergeStrategy :=  {
        case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
        case PathList(ps @ _*) if ps.last == "native-image.properties" => MergeStrategy.discard
        case PathList(ps @ _*) if ps.last == "reflection-config.json" => MergeStrategy.discard
        case PathList(ps @ _*) if ps.last == "io.netty.versions.properties" => MergeStrategy.first
        case PathList(ps @ _*) if ps.last == "reference-overrides.conf" => MergeStrategy.concat
        case x =>
          val oldStrategy = assemblyMergeStrategy.value
          oldStrategy(x)
      }
    )
    .dependsOn(`meetup-api`)
