package main

import spark._
import spark.SparkContext._

import scala.io._
import com.codahale.jerkson.Json._
//import com.rockymadden.stringmetric
//import com.rockymadden.stringmetric.similarity._
import scala.util.Random.nextInt
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io.PrintWriter
import java.io.File

import main.support._
import main.feature.Elem

object Main{
  
  //Logger.getLogger("spark").setLevel(Level.WARN)
  
  def main(args: Array[String]) {
    
    if (args.length < 3) {
      System.err.println("Usage: SparkKmeans fileSparkConf fileParameters")
      System.exit(-1)
    }

    val fileOut = args(0)
    val fileSparkConf = args(1)
    val fileParamConf = args(2)
 
    val s : Iterable[Elem] = MultiSparkKmeans(fileSparkConf, fileParamConf)
    
    val writer = new PrintWriter(new File(fileOut))
    writer.write(s.map(_.toString() + "\n").mkString)
    writer.close()

  }

}


object MultiSparkKmeans {
 /**
   * Run a Spark program 
   * @param config  config filename 
   */
  def apply(config: String, paramFile:String) = {
    // Set System Configuration parameters
    val conf = parse[Map[String, String]](Source.fromFile(config).mkString.trim)
    println(conf)
    for ( key <- List( "host", "inputFile","appName","initialCentroids","convergeDist" ) ) {
          if (!conf.contains(key)) {
             System.err.println("Missing configuration key '" ++ key ++ "' in ./spark.conf")
              sys.exit(1)
          }
      }

    val host = conf("host")
    val inputFile = conf("inputFile")
    val appName = conf("appName")

    // Set Algorithm parameters
    val k = conf("initialCentroids").toInt
    val convergeDist = conf("convergeDist").toDouble
    val weights = parse[Map[String, Double]](Source.fromFile(paramFile).mkString.trim)
 
    // Create a SparkContext Object to access the cluster
    //val sc = new SparkContext(host, appName, System.getenv("SPARK_HOME"), List("./target/job.jar") )
    val sc = new SparkContext(host, appName)
    
    //  Input Step
    val pointsRaw = Support.parser(inputFile, sc)
    //  Create a Vectorial Space with distance methods and weights
    val geometry = VSpace(weights.values.toList)
    
    println(Console.CYAN + "READ" + "-" * 100 + "\nRead " + pointsRaw.count() + " points." + Console.WHITE)
    if (Support.DEBUG){
      println(Console.CYAN+pointsRaw.collect().map(_.toString() + "\n").mkString)
      println(Console.WHITE)
    }
    
    //  Normalize Step
    val points = Support.normalizeInput(pointsRaw).cache()

    //  RDD Action to set k random centroids from points
    val centroids: Seq[Elem] = points.takeSample(withReplacement = false, num = k, seed = nextInt())
    
    // Run the kmeans algorithm 
    val resultCentroids = KMeans(points, centroids, convergeDist, geometry)
    
    //Centroids output  
    println(Console.GREEN)
    println("RESULT-CENTROIDS" + "-" * 100)
    println(resultCentroids.map(_.toString() + "\n").mkString)
    println(Console.WHITE)

    //Evaluation(points,centroids,geometry)
    resultCentroids
  }

}