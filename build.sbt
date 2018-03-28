import sbtassembly.PathList

//sbt "set test in assembly := {}" clean assembly
version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.8.1"  % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7"  % "provided",
  "org.specs2" %% "specs2-core" % "3.9.1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.codahale.metrics" % "metrics-core" % "3.0.2"  % "test",

  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0",
  "org.apache.spark" %% "spark-core"             % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-sql"             % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-streaming"       % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-streaming-kafka-0-10" % sparkVersion

)



// resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
}



