package main.feature

import main.support.ScaleInterval
import main.MultiSparkKmeans
/**
 * Common interface for feature type
 */
trait Feature{
}

/**
 * A  representation of a point in some n-dimensional space
 * @param id 
 * @param terms a sequence of Numeric features
 * @param categs a sequence of Categorical features
 * The list of these 2 types is useful to compute medoid for every Categorical
 */
case class Elem(val id : String, val terms: Seq[Numeric], val categs: Seq[Categorical]){
  override def toString = "\n[ID: "+id+"\n\tNumeric: "+printTerms(terms)+"\n\tCategs: "+categs+"]"

  def printTerms(terms: Seq[Numeric]) = {
    terms.map( n => n match {
      case _ if n.typeName == "IP" =>  "IP: "+IP.LongToString(n.terms(0).toLong)
      case _ => n
    })
  }
} 

/**
 * A feature to represent a numeric field that represent a MultiPoint in some space
 * @param typeName to identify the feature
 * @param terms a sequence of coordinates
 */
case class Numeric(val typeName: String, val terms: Seq[Double]) extends Feature {
  
  override def toString = typeName+": "+terms.map("%.4f ".format(_) + " ").mkString
  
  def +(that: Numeric) = Numeric(typeName, this.zip(that).map { case (a, b) => a + b })
  def -(that: Numeric) = Numeric(typeName, this.zip(that).map { case (a, b) => a - b })
  def /(divisor: Double) = Numeric(typeName, terms.map(_ / divisor))
  def zip(that: Numeric) = this.terms.zip(that.terms)
  def dotProduct(that: Numeric) = this.zip(that).map { case (x, y) => x * y }.sum
  //Weighted dot product
  //def WDotProduct(that: Numeric) = (this.terms, MultiSparkKmeans.wForNumeric(typeName), that.terms).zipped.map { case (x, w, y) => x * w * y }.sum
  
  
  //def maxLimit(that:Numeric) : Numeric = Numeric(typeName, this.zip(that).map { case (a, b) => if (a>b) a else b })
  lazy val abs = Numeric(typeName,terms.map(_.abs))
  lazy val norm = math.sqrt(this.dotProduct(this))
  lazy val numDimensions = terms.length
  lazy val sum = terms.sum
}

object Numeric{
  //Normalize a Numeric element
  def scaleNumeric(t: Numeric, max: Numeric, min: Numeric, scale: (Double, Double, Double) => Double): Numeric = {
    
    Numeric(t.typeName,
      for {
        i <- 0 to t.terms.size - 1; newTerms = scale(t.terms(i), max.terms(i), min.terms(i))
      } yield (newTerms))
  }

  def maxLimit(n1: Numeric, n2: Numeric) : Numeric = Numeric("", n1.zip(n2).map { case (a, b) => if (a>b) a else b })
  def minLimit(n1: Numeric, n2: Numeric) : Numeric = Numeric("", n1.zip(n2).map { case (a, b) => if (a<b) a else b })
}


/**
 * A feature to represent a categorical field
 * @param typeName to identify the feature
 * @param term the value of the feature
 */
case class Categorical(val typeName: String, val term: String) extends Feature{
  def maxLimit(that: Categorical) : Categorical = if (this.term.size > that.term.size) this else that
  def minLimit(that: Categorical) : Categorical = if (this.term.size < that.term.size) this else that
  override def toString = typeName+":"+term
}

