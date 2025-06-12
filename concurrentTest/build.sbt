name := "novel-crawler"
version := "1.0"
scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "org.jsoup" % "jsoup" % "1.14.3",
  "com.typesafe.akka" %% "akka-http" % "10.2.9",
  "com.typesafe.akka" %% "akka-stream" % "2.6.19",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.6.19",
  "com.typesafe.akka" %% "akka-slf4j" % "2.6.19",
  "ch.qos.logback" % "logback-classic" % "1.2.11"
)