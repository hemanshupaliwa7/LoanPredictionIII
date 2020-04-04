import AssemblyKeys._

lazy val buildSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "myorg",
  scalaVersion := "2.11.1"
)

val app = (project in file(".")).
  settings(buildSettings: _*).
  settings(assemblySettings: _*).
  settings(
    mergeStrategy in assembly := {
      //case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) =>
        xs map {_.toLowerCase} match {
          case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
            MergeStrategy.discard
          case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
            MergeStrategy.discard
          case "services" :: _ =>  MergeStrategy.filterDistinctLines
          case _ => MergeStrategy.first
        }
      case x => MergeStrategy.first
    }
  )

name := "LoanPredictionIII"

version := "1.0"

scalaVersion := "2.11.1"

val sparkVersion = "2.4.5"
val kafkaVersion = "0.10.0.1"
//val sparklingWaterVersion = "3.26.11-2.4"
val sparklingWaterVersion = "3.28.1.2-1-2.4"

//fork := true

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"


libraryDependencies ++= Seq(
  //Spark Libreries
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-repl" % sparkVersion,
  //Kafka Libreries
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  //Others
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.4.0",
  "net.liftweb" % "lift-json_2.11" % "3.0.1",
  "mysql" % "mysql-connector-java" % "5.1.12",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.6",
  "com.typesafe" % "config" % "1.2.1",
  "com.github.benfradet" %% "spark-kafka-writer" % "0.4.0",
  "com.m3" %% "curly-scala" % "0.5.+",
  "io.confluent" % "kafka-avro-serializer" % "3.2.2",
  "com.databricks" %% "spark-avro" % "3.2.0",
  "com.amazonaws" % "aws-java-sdk-kms" % "1.10.75.1",
  "org.scalaz" % "scalaz-core_2.11" % "7.3.0-M14",
  "com.ovoenergy" %% "kafka-serialization-avro4s" % "0.1.23",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "com.crealytics" %% "spark-excel" % "0.8.2",
  "org.apache.mesos" % "mesos" % "1.6.2",
  "com.google.protobuf" % "protobuf-java" % "3.9.1",
  //Sparkling Water
  "ai.h2o" %% "sparkling-water-core" % sparklingWaterVersion,
  "ai.h2o" %% "sparkling-water-utils" % sparklingWaterVersion,
  "ai.h2o" %% "sparkling-water-repl" % sparklingWaterVersion,
  "ai.h2o" %% "sparkling-water-ml" % sparklingWaterVersion,
  "ai.h2o" %% "sparkling-water-package" % sparklingWaterVersion,
  "ai.h2o" %% "sparkling-water-scoring" % sparklingWaterVersion,
  "org.codehaus.jsr166-mirror" % "jsr166y" % "1.7.0"
)


resolvers ++= Seq(
  //"JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Maven Central" at "https://repo.maven.apache.org/maven2",
  "Spray Repository" at "http://repo.spray.io/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  "confluent" at "http://packages.confluent.io/maven/",
  Resolver.bintrayRepo("ovotech", "maven")
)


