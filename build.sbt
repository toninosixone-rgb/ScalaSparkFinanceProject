// build.sbt
name := "ScalaSparkFinanceProject"
version := "1.0"
scalaVersion := "2.12.18" // Versione Scala moderna

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.2",
  "org.apache.spark" %% "spark-sql" % "3.4.2",
  "io.delta" %% "delta-core" % "2.4.0" // Per Delta Lake
)