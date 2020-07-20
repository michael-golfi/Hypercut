name := "Hypercut"

//Currently using 2.11 series for compatibility with Apache Spark images on Google Dataproc
//scalaVersion := "2.12.3"
scalaVersion := "2.11.11"

scalacOptions ++= Seq(
  "-optimize",
  "-feature",
  "-Yinline-warnings")

scapegoatVersion in ThisBuild := "1.3.9"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"

