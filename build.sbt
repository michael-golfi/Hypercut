import com.typesafe.sbt.SbtStartScript
import com.github.retronym.SbtOneJar._

oneJarSettings

seq(SbtStartScript.startScriptForClassesSettings: _*)

name := "DBPart"

//Currently using 2.11 series for compatibility with Apache Spark images on Google Dataproc
//scalaVersion := "2.12.3"
scalaVersion := "2.11.11"

libraryDependencies += "com.fallabs" % "kyotocabinet-java" % "latest.integration"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"

