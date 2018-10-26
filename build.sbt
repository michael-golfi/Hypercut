import com.typesafe.sbt.SbtStartScript
seq(SbtStartScript.startScriptForClassesSettings: _*)

name := "DBPart"

scalaVersion := "2.12.3"

libraryDependencies += "com.fallabs" % "kyotocabinet-java" % "latest.integration"


