name := "s3storageclass"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.11.321",
  "com.typesafe.akka" %% "akka-stream" % "2.5.12"
)