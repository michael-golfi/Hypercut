//Comment out the following line to speed up fetching dependencies,
//if source attachments are not needed.

EclipseKeys.withSource := true

name := "DBPart"

//Currently using 2.11 series for compatibility with Apache Spark images on Google Dataproc
//scalaVersion := "2.12.3"
scalaVersion := "2.11.11"

scalacOptions ++= Seq(
  "-optimize",
  "-feature",
  "-Yinline-warnings")

scapegoatVersion in ThisBuild := "1.3.9"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "com.fallabs" % "kyotocabinet-java" % "latest.integration"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"

libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"

libraryDependencies += "org.vegas-viz" %% "vegas" % "0.3.11"

