name := "from gvcf to ElasticSearch"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.3.1",
				       "org.apache.spark" %% "spark-sql" % "1.3.1")