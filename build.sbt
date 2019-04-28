name := "kafka-streams-one-to-many-join"

organization := "poc.svend"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

// to fix an issue
libraryDependencies ++= Seq (

  // Confluent 5.1.2 comes with kafka 2.1.1
  "org.apache.kafka" %% "kafka-streams-scala" % "2.2.0",
  "io.confluent" % "kafka-streams-avro-serde" % "5.2.1",

  // resolving a conflict with Confluent lib (https://docs.confluent.io/current/streams/developer-guide/dsl-api.html#streams-developer-guide-dsl-scala)
  "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts(Artifact("javax.ws.rs-api", "jar", "jar")),

  "org.slf4j" % "slf4j-simple" % "1.7.26",
  "com.github.scopt" %% "scopt" % "3.7.1",
  "com.typesafe.play" %% "play-json" % "2.7.3"
)

testOptions in Test += Tests.Argument("-oF")
