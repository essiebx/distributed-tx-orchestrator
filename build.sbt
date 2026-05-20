ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file(".")).aggregate(sharedEvents, mockServices, coreOrchestrator)
  .settings(
    name := "distributed-tx-orchestrator",
  )

lazy val sharedEvents = (project in file("shared-events"))
  .settings(
    name := "shared-events",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.21",
      "dev.zio" %% "zio-json" % "0.6.2"
    )
  )

lazy val coreOrchestrator = (project in file("core-orchestrator"))
  .dependsOn(sharedEvents)
  .settings(
    name := "core-orchestrator",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.21",
      "dev.zio" %% "zio-streams" % "2.0.21",
      "dev.zio" %% "zio-json" % "0.6.2",
      "dev.zio" %% "zio-http" % "3.0.0-RC4",
      "dev.zio" %% "zio-kafka" % "2.7.1",
      "io.getquill" %% "quill-jdbc-zio" % "4.8.1",
      "org.postgresql" % "postgresql" % "42.7.2"
    )
  )

lazy val mockServices = (project in file("mock-services"))
  .dependsOn(sharedEvents)
  .settings(
    name := "mock-services",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.21",
      "dev.zio" %% "zio-kafka" % "2.7.1",
      "dev.zio" %% "zio-json" % "0.6.2"
    )
  )
