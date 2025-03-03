ThisBuild / organization := "com.example"
ThisBuild / version := "0.0.1"

ThisBuild / scalaVersion := "2.13.16"

ThisBuild / libraryDependencySchemes +=
  "org.scala-lang.modules" %% "scala-java8-compat" % VersionScheme.Always

ThisBuild / resolvers += "Akka library repository".at("https://repo.akka.io/maven")

val macwire = "com.softwaremill.macwire" %% "macros" % "2.3.3" % "provided"
val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1" % Test
val postgres = "org.postgresql" % "postgresql" % "42.7.5"

val akkaVersion = "2.10.2"
val akkaHttpVersion = "10.7.0"
val akkaManagementVersion = "1.6.0"
val slf4jVersion = "2.0.16"

val overrideSettings = Seq(
  dependencyOverrides ++= Seq(
    "com.typesafe.akka" %% "akka-actor"                  % akkaVersion,
    "com.typesafe.akka" %% "akka-remote"                 % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster"                % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-sharding"       % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-tools"          % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-typed"          % akkaVersion,
    "com.typesafe.akka" %% "akka-coordination"           % akkaVersion,
    "com.typesafe.akka" %% "akka-discovery"              % akkaVersion,
    "com.typesafe.akka" %% "akka-distributed-data"       % akkaVersion,
    "com.typesafe.akka" %% "akka-serialization-jackson"  % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence"            % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence-query"      % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j"                  % akkaVersion,
    "com.typesafe.akka" %% "akka-stream"                 % akkaVersion,
    "com.typesafe.akka" %% "akka-protobuf-v3"            % akkaVersion,
    "com.typesafe.akka" %% "akka-actor-typed"            % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence-typed"      % akkaVersion,
    "com.typesafe.akka" %% "akka-multi-node-testkit"     % akkaVersion % Test,
    "com.typesafe.akka" %% "akka-testkit"                % akkaVersion % Test,
    "com.typesafe.akka" %% "akka-stream-testkit"         % akkaVersion % Test,
    "com.typesafe.akka" %% "akka-actor-testkit-typed"    % akkaVersion % Test,
    "com.typesafe.akka" %% "akka-http-core"              % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-spray-json"        % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http"                   % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-parsing"                % akkaHttpVersion,
    "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % akkaManagementVersion,
    "com.lightbend.akka.management" %% "akka-management-cluster-http" % akkaManagementVersion,
    "com.lightbend.akka.management" %% "akka-management"            % akkaManagementVersion,
    "ch.qos.logback" % "logback-classic" % "1.5.16",
    "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
    "org.slf4j" % "jul-to-slf4j" % slf4jVersion,
    "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
  ))

lazy val meetup =
  (project in file("."))
    .aggregate(`meetup-api`, `meetup-impl`)

lazy val `meetup-api` =
  (project in file("meetup-api"))
    .settings(
      libraryDependencies ++= Seq(
        lagomScaladslApi),
      scalaVersion := "2.13.16"
    ).settings(overrideSettings)

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
    .settings(overrideSettings)
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
