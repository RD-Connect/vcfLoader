name := "from gvcf to ElasticSearch"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.4.1",
                            "com.sksamuel.elastic4s" %% "elastic4s" % "1.4.0",
				                    "org.apache.spark" %% "spark-sql" % "1.4.1",
                            "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test" ,
                            "org.bdgenomics.adam" % "adam-core_2.10" % "0.17.1")