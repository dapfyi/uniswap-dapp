name := "Uniswap"
version := "0.1.0"

sbtVersion := "1.5.5"
scalaVersion := "2.12.15"

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.2.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "3.2.0" % "provided",
    "io.delta" %% "delta-core" % "1.1.0" % "provided",
    "org.apache.kafka" % "kafka-clients" % "2.8.0" % "provided",
    "com.holdenkarau" %% "spark-testing-base" % "3.2.0_1.1.1" % Test excludeAll(
        ExclusionRule(organization = "org.apache.spark"))
)

Test / fork := true
Test / parallelExecution := false
Test / envVars := Map("DATA_BUCKET" -> "dummy", "DELTA_BUCKET" -> "dummy")
Test / javaOptions ++= Seq("-Xms6G", "-Xmx6G", "-XX:+CMSClassUnloadingEnabled", 
    "-Dspark.driver.dap.epoch=0")

