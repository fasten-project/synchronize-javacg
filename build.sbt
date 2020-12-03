name := "synchronize-javacg"
version := "0.1"
scalaVersion := "2.12.11"
val flinkVersion = "1.11.2"

// logging dependencies
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

// test dependencies
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test
libraryDependencies += "io.github.embeddedkafka" %% "embedded-kafka" % "2.6.0" % Test

// extra kafka dependency
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.6.0"


// argument parsing
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" % "flink-formats" % flinkVersion,
  "org.apache.flink" % "flink-json" % flinkVersion,
  "org.apache.flink" %% "flink-statebackend-rocksdb" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

assembly / mainClass := Some("eu.fasten.synchronization.Main")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// disabel parallel execution
Test / parallelExecution := false

// stays inside the sbt console when we press "ctrl-c" while a Flink program executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)