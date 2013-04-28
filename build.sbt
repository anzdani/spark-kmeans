name := "spark_kmeans"

version := "1.0"

scalaVersion := "2.9.2"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.7.0"

libraryDependencies += "com.rockymadden.stringmetric" % "stringmetric-core" % "0.25.1" 

libraryDependencies += "com.codahale" % "jerkson_2.9.1" % "0.5.0"

resolvers ++= Seq("Akka Repository"
at "http://repo.akka.io/releases/", "Spray Repository"
at "http://repo.spray.cc/")

resolvers += "repo.codahale.com" at "http://repo.codahale.com"
 

