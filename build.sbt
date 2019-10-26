name := "BGC Test"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-core" % "4.6.0" % "test",
  "org.apache.spark" %% "spark-sql" % "2.4.4"
)

scalacOptions += "-Ypartial-unification"

scalacOptions += "-Yrangepos"

fork in run := true

fork in Test := true
