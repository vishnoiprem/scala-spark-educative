name := "scala-spark-educative"
//version := "0.1"
//scalaVersion := "2.13.8"

//libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.11" % "2.1.0")

version := "1.0"

scalaVersion := "2.12.10"
//scalaVersion := "2.13.6"



libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0-preview2"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.4.13"
libraryDependencies += "org.apache.hbase" % "hbase" % "2.4.13"
//libraryDependencies += "ch.cern.hbase.connectors.spark" % "hbase-spark" % "1.0.1_spark-3.0.1_2"