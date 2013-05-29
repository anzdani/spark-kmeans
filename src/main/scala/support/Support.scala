package main.support

import spark._
import spark.SparkContext._
import scala.io._


import main.feature._

object Support {
  val DEBUG = false
  val DEBUGCENTROID = false
  
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

    if (Support.DEBUG) {
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
