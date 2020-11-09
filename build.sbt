lazy val sparkVersion = settingKey[String]("the spark version")

name := "spark-denormalize"
organization := "io.github.spark-denormalize"
licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

version := "0.1.0-SNAPSHOT"
sparkVersion := "3.0.0"

scalaVersion := "2.12.12"
crossScalaVersions := Seq("2.11.12", "2.12.12")

scalacOptions ++= Seq("-target:jvm-1.8")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
    // spark
    "org.apache.spark" %% "spark-core" % sparkVersion.value,
    "org.apache.spark" %% "spark-sql"  % sparkVersion.value,
)