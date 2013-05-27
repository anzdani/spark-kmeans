package main.feature

import main.support.Normalize
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
@serializable case class Elem(val id : String, val terms: Seq[Numeric], val categs: Seq[Categorical]){
  override def toString = "\n[ID: "+id+"\n\tNumeric: "+terms+"\n\tCategs: "+categs+"]"
} 

/**
 * A feature to represent a numeric field that represent a MultiPoint in some space
 * @param typeName to identify the feature
 * @param terms a sequence of coordinates
 */
case class Numeric(val typeName: String, val terms: Seq[Double]) extends Feature {
  override def toString = typeName+": "+terms.map(_.toString() + " ").mkString
  
  def +(that: Numeric) = Numeric(typeName, this.zip(that).map { case (a, b) => a + b })
  def -(that: Numeric) = Numeric(typeName, this.zip(that).map { case (a, b) => a - b })
  def /(divisor: Double) = Numeric(typeName, terms.map(_ / divisor))
  def zip(that: Numeric) = this.terms.zip(that.terms)
  def dotProduct(that: Numeric) = this.zip(that).map { case (x, y) => x * y }.sum
  //Weighted dot product
  def WDotProduct(that: Numeric) = (this.terms, MultiSparkKmeans.wForNumeric(typeName), that.terms).zipped.map { case (x, w, y) => x * w * y }.sum
  
  def minLimit(that:Numeric) : Numeric = Numeric(typeName, this.zip(that).map { case (a, b) => if (a<b) a else b })
  
  def maxLimit(that:Numeric) : Numeric = Numeric(typeName, this.zip(that).map { case (a, b) => if (a>b) a else b })
  lazy val abs = Numeric(typeName,terms.map(_.abs))
  lazy val norm = math.sqrt(this.WDotProduct(this))
  lazy val numDimensions = terms.length
  lazy val sum = terms.sum
}

object Numeric{
  //val weights : spark.broadcast.Broadcast[List[Double]] =
  //Normalize a Numeric element
  def normalizeNumeric(t: Numeric, max: Numeric, min: Numeric): Numeric = {
    Numeric(t.typeName,
      for {
        i <- 0 to t.terms.size - 1; newTerms = Normalize(t.terms(i), max.terms(i), min.terms(i), 1, 0)
      } yield (newTerms))
  }
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

