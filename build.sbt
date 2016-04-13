name := "from gvcf to ElasticSearch"

version := "1.0"

fork := true

parallelExecution in Test := false

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.0",
				                    "org.apache.spark" %% "spark-sql" % "1.6.0",
                            "org.apache.spark" %% "spark-streaming" % "1.6.0",
                            "org.apache.spark" %% "spark-hive" % "1.6.0",
                            "org.apache.spark" %% "spark-catalyst" % "1.6.0",
                            "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test" ,
                            //"org.bdgenomics.adam" % "adam-core_2.10" % "0.17.1",
                            "com.sksamuel.elastic4s" % "elastic4s-core_2.10" % "1.5.15",
                            "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.0",
                            "com.typesafe" % "config" % "1.3.0",
                            "info.cukes" % "cucumber-java8" % "1.2.3",
                            "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.2"
)
