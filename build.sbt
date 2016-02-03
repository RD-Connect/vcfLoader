name := "from gvcf to ElasticSearch"

fork := true

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.0",
				                    "org.apache.spark" %% "spark-sql" % "1.6.0",
                            "org.apache.spark" %% "spark-streaming" % "1.6.0",
                            "org.apache.spark" %% "spark-hive" % "1.6.0",
                            "org.apache.spark" %% "spark-catalyst" % "1.6.0",
                            "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test" ,
                            "org.bdgenomics.adam" % "adam-core_2.10" % "0.17.1",
                            "com.sksamuel.elastic4s" % "elastic4s-core_2.10" % "1.5.15",
                            "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.0"
)
