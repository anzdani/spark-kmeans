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

import main.support._
import main.feature.Elem

/**
 * LogHelper is a trait you can mix in to provide easy log4j logging 
 * for your scala classes. 
 **/
trait LogHelper {
    val loggerName = this.getClass.getName
    lazy val logger = Logger.getLogger(loggerName)
}

object Main extends LogHelper{
  
  Logger.getLogger("spark").setLevel(Level.WARN)
  
  def main(args: Array[String]) {
   
    logger.debug("entering someMethod")
    MultiSparkKmeans("./spark.conf")
  }

}


object MultiSparkKmeans {
  /**
   * Run a Spark program 
   * @param config  config filename 
   */
  def apply(config: String) = {
    
    // Set System Configuration parameters
    val conf = parse[Map[String, String]](Source.fromFile(config).mkString.trim)
    val host = conf.get("host").get.toString
    val inputFile = conf.get("inputFile").get.toString
    val appName = conf.get("appName").get.toString
    
    // Set Algorithm parameters
    val k = conf.get("initialCentroids").get.toInt
    val convergeDist = conf.get("convergeDist").get.toDouble
    
    // Create a SparkContext Object to access the cluster
    val sc = new SparkContext(host, appName)

    //  Input Step
    val pointsRaw = Support.parser(inputFile, sc)
    //  Create a Vectorial Space with distance methods and weights
    val geometry = VSpace(Support.weights)
    
    println(Console.CYAN + "READ" + "-" * 100 + "\nRead " + pointsRaw.count() + " points." )
    if (Support.DEBUG){
      println(pointsRaw.collect().map(_.toString() + "\n").mkString)
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

    Evaluation(points,centroids,geometry)

  }

}