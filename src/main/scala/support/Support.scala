package main.support

import spark._
import spark.SparkContext._
import scala.io._

import main.feature._

object Support {
  val DEBUG = false
  val DEBUGCENTROID = false

  def findElem(points: RDD[Elem], id: String, method: (Numeric, Numeric) => Numeric): Elem = {
    /*  RDD Action to compute max or min values for every feature of Elem  */
    points.reduce(
      (a: Elem, b: Elem) => {
        Elem(id, (a.terms, b.terms).zipped.map(method(_, _)), a.categs)
        //(a.categs, b.categs).zipped.map(_ maxLimit _))
      })
  }

  /**
   * Normalize input data with a scale transformation for every numerical feature
   * from a min-max interval to 0-1 one to have comparable values.
   * No normalization is performed on Categorical feature for this configuration of kmeans
   *
   * @param points  a RDD
   * @return a RDD
   */
  def scaleInput(points: RDD[Elem], elMax: Elem, elMin: Elem): RDD[Elem] = {
    /*  RDD Transformation to scale numerical features */
    points.map(el => {
      val newNterms: Seq[Numeric] = for {
        
        i <- 0 to el.terms.size - 1; nterms = 
        Numeric.scaleNumeric(el.terms(i), elMax.terms(i), elMin.terms(i), ScaleInterval(_,_,_,newMax=1,newMin=0))

      } yield (nterms)
      Elem(el.id, newNterms, el.categs)
    })
  }
  
  def scaleOutput(points: Seq[Elem], elMax: Elem, elMin: Elem): Seq[Elem] = {
    points.map(el => {
      val newNterms: Seq[Numeric] = for {
        
        i <- 0 to el.terms.size - 1; nterms = 
         Numeric.scaleNumeric(el.terms(i), elMax.terms(i), elMin.terms(i), ScaleInterval(_,max=1,min=0,_,_))
      
      } yield (nterms)
      Elem(el.id, newNterms, el.categs)
    })
  }

}

/**
 *  A object to provide Normalization of a value from an interval to a new one
 */
object ScaleInterval {
  def apply(x: Double, max: Double, min: Double, newMax: Double, newMin: Double) =
    ((x - min) / (max - min)) * (newMax - newMin) + newMin
}
