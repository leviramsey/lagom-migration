addSbtPlugin("com.lightbend.lagom" % "lagom-sbt-plugin" % "1.6.7")

ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")

addDependencyTreePlugin
