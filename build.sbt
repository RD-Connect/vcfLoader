import sbtassembly.AssemblyPlugin.autoImport._


name := "from gvcf to ElasticSearch"

version := "1.0"

fork := true

val sparkVers = "2.0.1"

parallelExecution in Test := false

scalaVersion := "2.11.8"

val sparkVersion = "2.0.2"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.5.0",
  "org.elasticsearch" % "elasticsearch" % "5.5.0"
)


libraryDependencies += "com.typesafe.play" %% "play-ws" % "2.4.3"
