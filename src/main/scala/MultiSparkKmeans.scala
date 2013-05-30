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
import main.support.Support._
import main.feature._

object Main{
  
  //Logger.getLogger("spark").setLevel(Level.WARN)
  
  def main(args: Array[String]) {
    
    if (args.length < 3) {
      System.err.println("Usage: SparkKmeans outFile fileSparkConf fileParameters")
      System.exit(-1)
    }

    val fileOut = args(0)
    val fileSparkConf = args(1)
    val fileParamConf = args(2)
 
    val s = MultiSparkKmeans(fileSparkConf, fileParamConf)
    
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
    for ( key <- List( "host", "inputFile","appName","initialCentroids","convergeDist","numSamplesForMedoid" ) ) {
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
    //It is the trust Level ForMedoid
    val numSamplesForMedoid = conf("numSamplesForMedoid").toInt
    val weights = parse[Map[String, Double]](Source.fromFile(paramFile).mkString.trim)
 
    // Create a SparkContext Object to access the cluster
    //val sc = new SparkContext(host, appName, System.getenv("SPARK_HOME"), List("./target/job.jar") )
    val sc = new SparkContext(host, appName)
    
    //  Create a Vectorial Space with distance methods and weights
    val geometry = VSpace(weights.values.toList, numSamplesForMedoid)
    
    // Create a test object to verify whether number of features and weights match or not
    val test = ElemFormat.construct1("{}")
    require(geometry.weights.size == (test.terms.size + test.categs.size), "Error: wrong number of features:")
    require( test.categs.size > 0 || (test.categs.size == 0 && geometry.numSamplesForMedoid == 0) , "Error: wrong number of features:")
    
    //  Input Step
    /*  RDD Creation:  Parse input file into a RDD  */
    /*  RDD Transformation:  Featurization of dataset  */
    val pointsRaw = sc.textFile(inputFile).map(line => {
      //every line is a json object
      ElemFormat.construct1(line)
    })

    println(Console.CYAN + "READ" + "-" * 100 + "\nRead " + pointsRaw.count() + " points." + Console.WHITE)
    if (Support.DEBUG){
      println(Console.CYAN+pointsRaw.collect().map(_.toString() + "\n").mkString)
      println(Console.WHITE)
    }
    
    //  Normalize Step
    val elMax : Elem = findElem(pointsRaw, "max", Numeric.maxLimit)
    val elMin : Elem = findElem(pointsRaw, "min", Numeric.minLimit)
    val points = Support.scaleInput(pointsRaw, elMax, elMin).cache()

    //  RDD Action to set k random centroids from points
    val centroids = points.takeSample(withReplacement = false, num = k, seed = nextInt())
    
    // Run the kmeans algorithm 
    val resultCentroids = KMeans(points, centroids, convergeDist, geometry)
    val result = scaleOutput(resultCentroids,elMax,elMin)
    
    //Centroids output  
    println(Console.GREEN)
    println("RESULT-CENTROIDS" + "-" * 100)
    println(result.map(_.toString() + "\n").mkString)
    println(Console.WHITE)

    //Evaluation(points,centroids,geometry)
    result

  }

}