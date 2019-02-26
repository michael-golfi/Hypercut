import com.typesafe.sbt.SbtStartScript
import com.github.retronym.SbtOneJar._

oneJarSettings

seq(SbtStartScript.startScriptForClassesSettings: _*)

name := "DBPart"

scalaVersion := "2.12.3"

libraryDependencies += "com.fallabs" % "kyotocabinet-java" % "latest.integration"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

