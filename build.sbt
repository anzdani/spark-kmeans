import sbt._
import AssemblyKeys._


name := "spark_kmeans"

version := "1.0"

scalaVersion := "2.9.2"

//assemblySettings
seq(assemblySettings: _*)


libraryDependencies ++= Seq( 
	"org.spark-project" %% "spark-core" % "0.7.0",
	"com.codahale" % "jerkson_2.9.1" % "0.5.0",
	"org.scalatest" %% "scalatest" % "1.9.1" % "test",
	"com.rockymadden.stringmetric" % "stringmetric-core" % "0.19.1",
	//"com.eed3si9n" % "sbt-assembly" % "0.8.8",
	"org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.2.0",
	"org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.2.0"
	)

resolvers ++= Seq(
	"Akka Repository" at "http://repo.akka.io/releases/", 
	"Spray Repository" at "http://repo.spray.cc/",
	"repo.codahale.com" at "http://repo.codahale.com",
	"Maven Central Server" at "http://repo1.maven.org/maven2",
	"Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
	"artifactory" at "http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"
	)

scalacOptions += "-deprecation"

mainClass in assembly := Some("main.Main")

test in assembly := {}

//jarName in assembly := "job.jar"

mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
