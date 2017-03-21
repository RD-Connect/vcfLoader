name := "from gvcf to ElasticSearch"

version := "1.0"

fork := true

val sparkVers = "2.0.1"

parallelExecution in Test := false

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.0.1" % "provided" ,
  "org.apache.spark" %% "spark-sql" % "2.0.1" % "provided" ,
  "org.apache.spark" %% "spark-streaming" % "2.0.1" % "provided" ,
  "org.apache.spark" %% "spark-hive" % "2.0.1" % "provided" ,
  "org.apache.spark" %% "spark-catalyst" % "2.0.1" % "provided",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test" ,
  //"org.bdgenomics.adam" % "adam-core_2.10" % "0.17.1",
  //"com.sksamuel.elastic4s" % "elastic4s-core_2.10" % "2.2.1",
  "com.sksamuel.elastic4s" %% "elastic4s-core_2.11" % "2.4.0",
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.2.0" ,
  //    "org.elasticsearch" % "elasticsearch" % "2.2.1",
  "com.typesafe" % "config" % "1.3.0"
  //   "org.apache.lucene" % "lucene-core" % "5.4.1"
)

// Skip tests when assembling fat JAR
test in assembly := {}

// Exclude jars that conflict with Spark (see https://github.com/sbt/sbt-assembly)
libraryDependencies ~= { _ map {
  case m if Seq("org.elasticsearch").contains(m.organization) =>
    m.exclude("commons-logging", "commons-logging").
      exclude("commons-collections", "commons-collections").
      exclude("commons-beanutils", "commons-beanutils-core").
      exclude("com.esotericsoftware.minlog", "minlog").
      exclude("joda-time", "joda-time").
      exclude("org.apache.commons", "commons-lang3").
      exclude("org.apache.spark", "spark-network-common_2.10").
      exclude("org.slf4j", "slf4j-api")
  case m => m
}}

assemblyJarName in assembly := "spark-duplicate-detection-assembly.jar"

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}
