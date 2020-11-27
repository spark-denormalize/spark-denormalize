lazy val sparkVersion = settingKey[String]("the spark version")
lazy val jacksonVersion = settingKey[String]("the jackson version")

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "io.github.spark-denormalize"
ThisBuild / licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

ThisBuild / sparkVersion := "3.0.0"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / jacksonVersion := "2.11.4"
ThisBuild / crossScalaVersions := Seq("2.11.12", "2.12.12")
ThisBuild / scalacOptions ++= Seq("-target:jvm-1.8")
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// scalafmt: { maxColumn = 200 }
lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "spark-denormalize",
    //
    // sbt-buildinfo
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, sparkVersion),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoPackage := "io.github.sparkdenormalize",
    buildInfoUsePackageAsPath := true,
    //
    // dependencies - provided
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
    //
    // dependencies
    libraryDependencies += "io.circe" %% "circe-core" % "0.13.0",
    libraryDependencies += "io.circe" %% "circe-generic" % "0.13.0",
    libraryDependencies += "io.circe" %% "circe-generic-extras" % "0.13.0",
    libraryDependencies += "io.circe" %% "circe-parser" % "0.13.0",
    libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0",
    libraryDependencies += "org.jgrapht" % "jgrapht-io" % "1.4.0",
    libraryDependencies += "org.rogach" %% "scallop" % "3.5.1",
    libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.3" % "test",
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion.value,
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion.value,
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion.value,
    libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion.value,
    libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion.value,
    //
    // task - compile
    (compile in Compile) := (compile in Compile).dependsOn((scalastyle in Compile).toTask("")).value,
    (compile in Compile) := (compile in Compile).dependsOn(scalafmtCheck in Compile).value,
    //
    // task - test
    (fork in Test) := true,
    (parallelExecution in Test) := false,
    (scalastyleFailOnError in Test) := true,
    (scalastyleFailOnWarning in Test) := true,
    (test in Test) := (test in Test).dependsOn((scalastyle in Test).toTask("")).value,
    (test in Test) := (test in Test).dependsOn(scalafmtCheck in Test).value,
    //
    // task - package
    (artifactName in packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      s"${artifact.name}_${module.revision}_spark-${sparkVersion.value}_scala-${sv.binary}.${artifact.extension}"
    },
    //
    // task - assembly
    (test in assembly) := {}, // skip testing during assembly
    (assemblyOption in assembly) := (assemblyOption in assembly).value.copy(includeScala = false),
    (assemblyJarName in assembly) := {
      s"${name.value}-assembly_${version.value}_spark-${sparkVersion.value}_scala-${scalaBinaryVersion.value}.jar"
    },
    (assemblyMergeStrategy in assembly) := {
      // fixes `com.fasterxml.jackson`
      case PathList("module-info.class") => MergeStrategy.discard
      case x                             => (assemblyMergeStrategy in assembly).value(x)
    }
  )
