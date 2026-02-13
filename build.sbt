resolvers ++= Seq(
  Resolver.mavenLocal,
  "Maven Central Server" at "https://repo1.maven.org/maven2",
  "Typesafe Repository" at "https://dl.bintray.com/typesafe/maven-releases/",
  "confluent" at "https://packages.confluent.io/maven/"
)

lazy val confluentVersion = "7.3.0"
lazy val confluentAvroVersion = "7.3.0"
lazy val reactiveStreamsVersion = "1.0.4"
lazy val akkaHttpVersion = "10.5.0-M1"
lazy val akkaManagementVersion = "1.2.0"
lazy val liftJsonVersion = "3.5.0"
lazy val alpakkaCassandraVersion = "5.0.0"
lazy val akkaStreamKafka = "4.0.0"
lazy val json4sVersion = "4.1.0-M2"
lazy val logbackVersion = "1.4.5"
lazy val jodaTime = "2.12.2"
lazy val logbackKafkaAppenderVersion = "0.2.0-RC2"
lazy val alpakkaSlickVersion = "5.0.0"
lazy val Avro4sVersion = "4.1.0"
lazy val akkaVersion = "2.8.0-M1"

lazy val project_bi_msv023 = (project in file("."))
  .settings(
    name := "Project BI / MSV-023",
    organization := "com.github.magkairatov",
    scalaVersion := "2.13.10",
    Compile / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint", "-encoding", "UTF-8", "-J-Xss100m"),
    Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    run / javaOptions ++= Seq("-Xms128m", "-Xmx1024m", "-Xss80m", "-Djava.library.path=./target/native"),

    libraryDependencies ++= Seq(
      "io.confluent.ksql" % "ksqldb-api-client" % confluentVersion,
      "io.confluent" % "kafka-avro-serializer" % confluentAvroVersion,
      "org.reactivestreams" % "reactive-streams" % reactiveStreamsVersion,
      "com.sksamuel.avro4s"   %% "avro4s-core" % Avro4sVersion,
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % alpakkaCassandraVersion,
      "com.datastax.cassandra" % "cassandra-driver-extras" % "3.11.3",
      "org.apache.kafka" % "kafka-streams" % "3.3.1",

      "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafka,
      "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
      "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % akkaManagementVersion,
      "com.lightbend.akka.management" %% "akka-management" % akkaManagementVersion,
      "com.lightbend.akka.management" %% "akka-management-cluster-http" % akkaManagementVersion,
      "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % akkaManagementVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-slick" % alpakkaSlickVersion,
      "joda-time" % "joda-time" % jodaTime,
      "org.json4s" %% "json4s-jackson" % json4sVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "net.liftweb" %% "lift-json" % liftJsonVersion,
      "net.liftweb" %% "lift-json-ext" % liftJsonVersion,
      "com.github.danielwegener" % "logback-kafka-appender" % logbackKafkaAppenderVersion,
      "eu.timepit" %% "refined" % "0.10.1",
      "org.typelevel" %% "cats-effect" % "3.5-6581dc4",
      "com.oracle.database.jdbc" % "ojdbc8" % "21.9.0.0",
      "com.oracle.ojdbc" % "orai18n" % "19.3.0.0",
      "javax.mail" % "mail" % "1.5.0-b01"
    )
  )

enablePlugins(JavaAppPackaging,DockerPlugin,AshScriptPlugin)

Compile / mainClass := Some("com.github.magkairatov.Entry")

//dockerBaseImage := "openjdk:20-jdk-oraclelinux8"
dockerBaseImage := "adoptopenjdk/openjdk15:alpine-slim"
//dockerBaseImage := "adoptopenjdk/openjdk15"
//dockerBaseImage := "adoptopenjdk/openjdk15:x86_64-centos-jre-15.0.2_7"
Docker / daemonUser := "msv023"
Docker / packageName  := "project_bi_msv023"
Docker / version  := (project_bi_msv023 / version).value
dockerUpdateLatest := true
dockerExposedPorts := Seq(6677,7766)
dockerRepository := Some("magkairatov")