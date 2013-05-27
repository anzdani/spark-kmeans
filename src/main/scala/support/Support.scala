package main.support

import spark._
import spark.SparkContext._
import scala.io._

import com.codahale.jerkson.Json._
import main.feature._

object Support{
 val DEBUG = false
 val DEBUGCENTROID = false
 
 /**
   * Read input file that is in json format  
   * @param input   filename
   * @param sc      spark context handle
   * @return        a RDD of Element
   */
  def parser(input: String, sc: SparkContext): RDD[Elem] = {
    //Extract methods to get data from different type value
    //obtained from loading json object in a Map 
    def extractFromArray(key: String, m: Map[String, Any], default: String): List[String] =
      m.get(key).getOrElse("[" + default + ",]").toString.drop(1).dropRight(1).split(",").toList
    
    def extractFromValue(key: String, m: Map[String, Any], default: String): String = { 
      val s = m.get(key).getOrElse(default).toString
      if (s.isEmpty) "0.0"
      else s
     } 
    
    //TODO: for map if many uri
    def extractFromMap(key: String, m: Map[String, Any], default: String) =
      m.get(key).getOrElse("0=" + default + "}").toString.split("=")(1).split("}")(0)


    /* Define element feature types and weights*/
    //Partial constructor to use in parser
    //Decide here what feature you want to select from input
    //typename is used in distanceOnFeature of VSpace in the pattern matching  
    val n = Numeric(typeName = "numeric", _: Seq[Double])
    val ip = Categorical(typeName = "IP", _: String)
    val bot = Categorical(typeName = "bot", _: String)
    val uri = Categorical(typeName = "uri", _: String) 
    //TODO: Create automatically the constructor and parser for a customized Elem


    /*  RDD Creation:  Parse input file into a RDD  */
    /*  RDD Transformation:  Featurization of dataset  */
    val data : RDD[Elem] = sc.textFile(input).map(line => {
      //every line is a json object
      val m = parse[Map[String, Any]](line)
      Elem(
        extractFromMap("_id", m, "0"),
        List(
          n(List(
            extractFromValue("long", m, "0.0").toDouble,
            extractFromValue("lat", m, "0.0").toDouble,
            extractFromMap("date", m, "0.0").toDouble))
            ),
        List(
          ip(extractFromValue("IP", m, "")),
          bot(extractFromValue("bot", m, "")),
          uri(extractFromArray("uri", m, "")(0))))
    })
    data
  }


  /**
   * Normalize input data with a scale transformation for every numerical feature
   * from a min-max interval to 0-1 one to have comparable values.
   * No normalization is performed on Categorical feature for this configuration of kmeans
   *
   * @param points  a RDD
   * @return a RDD 
   */ 
  def normalizeInput(points: RDD[Elem]): RDD[Elem] = {

    /*  RDD Action to compute max values for every feature of Elem  */
    val elMax: Elem = points.reduce(
      (a: Elem, b: Elem) => {
        Elem("max", (a.terms, b.terms).zipped.map(_ maxLimit _), a.categs)
          //(a.categs, b.categs).zipped.map(_ maxLimit _))
      })
    
    /*  RDD Action to compute min values for every feature of Elem  */
    val elMin = points.reduce(
      (a: Elem, b: Elem) => {
        Elem("min", (a.terms, b.terms).zipped.map(_ minLimit _), a.categs)
          //(a.categs, b.categs).zipped.map(_ minLimit _))
      })

    if (Support.DEBUG){
      println(Console.YELLOW)
      println("Max-Min" + "-" * 100)
      println(elMax)
      println(elMin)
    }

    /*  RDD Transformation to scale numerical features */
    points.map(el => {
      val newNterms: Seq[Numeric] = for {
        i <- 0 to el.terms.size - 1; nterms = Numeric.normalizeNumeric(el.terms(i), elMax.terms(i), elMin.terms(i))
      } yield (nterms)
      Elem(el.id, newNterms, el.categs)
    })
  }

 }

/**
 *  A object to provide Normalization of a value from an interval to a new one 
 */
object Normalize {
  def apply(x: Double, max: Double, min: Double, newMax: Double, newMin: Double) =
    ((x - min) / (max - min)) * (newMax - newMin) + newMin
}
