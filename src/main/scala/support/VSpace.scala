package main.support

import main.feature._
import main.distance._

/**
 * A trait to represent a general geometry
 */
trait VectorSpace[T] {
  def distance(x: T, y: T): Double
  def centroid(ps: Seq[T]): Option[T]
}

/**
 * A case class to define a Vector Space for Elem types
 * @param weights   a list of weights for features of Elem
 */
@serializable case class VSpace(val weights: List[Double], val numSamplesForMedoid: Int) extends VectorSpace[Elem] {
  /**
   * Compute distance on feature according to different type
   * Distance can be customized with the pattern matching
   */
  def distanceOnFeature(f1: Feature, f2: Feature): Double = (f1, f2) match {
    /**
     * Distances chosen for categorical types
     */
    case (c1: Categorical, c2: Categorical) => (c1, c2) match {
      case _ if c1.typeName == "ip" => 1 - IP.similarity(c1.term, c2.term)
      case _ if c1.typeName == "bot" => StringDistance("normalizedLevenshtein")(c1.term, c2.term)
      case _ if c1.typeName == "uri" => StringDistance("normalizedLevenshtein")(c1.term, c2.term)
      case _ => StringDistance("normalizedLevenshtein")(c1.term, c2.term)
    //you can change distance type
    }

    /**
     * Distances chosen for numerical types
     */
    case (n1: Numeric, n2: Numeric) => (n1, n2) match {
      case _ if n1.typeName == "numeric" => ScaleInterval(NumericDistance("euclidean")(n1, n2), max = math.sqrt(2), min = 0, newMax = 1, newMin = 0)
      case _ if n1.typeName == "space" => ScaleInterval(NumericDistance("euclidean")(n1, n2), max = math.sqrt(2), min = 0, newMax = 1, newMin = 0)
      case _ if n1.typeName == "time" => ScaleInterval(NumericDistance("euclidean")(n1, n2), max = math.sqrt(2), min = 0, newMax = 1, newMin = 0)
      case _ if n1.typeName == "IP" => ScaleInterval(NumericDistance("euclidean")(n1, n2), max = math.sqrt(2), min = 0, newMax = 1, newMin = 0)
      case _ => ScaleInterval(NumericDistance("euclidean")(n1, n2), max = math.sqrt(2), min = 0, newMax = 1, newMin = 0)
    //you can change distance type
    }
    case _ => System.err.println("Wrong Feature Type"); sys.exit(1)
  }

  /**
   * Compute distance on a Elem
   * with a weighted distance on all feature
   */
  def distance(el1: Elem, el2: Elem): Double = {
    val ndist: Seq[Double] = (el1.terms, el2.terms).zipped.map(distanceOnFeature(_, _))
    val cdist: Seq[Double] = if (numSamplesForMedoid != 0) (el1.categs, el2.categs).zipped.map(distanceOnFeature(_, _)) else Seq()
    //weighted distance
    val d = ndist ++ cdist
    (d, weights).zipped.map(_ * _).sum/d.size
  }

  /**
   * An implementation of centroid method for kmeans algorithm
   * that computes numerical centroid and medoid for categorical features
   * @param points  a list of elements
   * @return a Option: an Elem or None if points is empty
   */
  def centroid(points: Seq[Elem]): Option[Elem] = {
    //compute new centroid for numeric part

    if (points.isEmpty) return None

    val seqTerms: Seq[Seq[Numeric]] = points.map(x => x.terms)
    val newCentroid = seqTerms.reduce((a, b) => (a, b).zipped.map(_ + _))

    if (Support.DEBUGCENTROID) {
      println(Console.CYAN)
      println("NEW MEDOID PART" + "-" * 100)
    }

    //select only p closest elements to centroid
    var categs: Seq[Categorical] = List()
    if (numSamplesForMedoid != 0) {
      //compute new centroid for categorical part
      for (i <- points(0).categs.size - 1 to 0 by -1) {

        val seqCategs: Seq[Categorical] = points.take(math.min(numSamplesForMedoid, points.size)).map(x => x.categs(i))
        //call newMedoid on seq of Categorical of the same type
        val cat: Categorical = newMedoid(seqCategs)._1
        //pre-append element to avoid to traverse all list
        categs = cat +: categs
        if (Support.DEBUGCENTROID) {
          println("new:\t\t" + cat)
          println("seqCategs:\t" + seqCategs)
          println("add part:\t" + categs)
        }
      }
    }
    
    //compute Medoid on a sequence of categorical of the same type
    def newMedoid(categs: Seq[Categorical]): (Categorical, Double) = {
      def sumDistance(c: Categorical): Double = {
        if (categs.isEmpty) return 0
        val cc = categs.map(distanceOnFeature(_, c))
        /*
          println(Console.GREEN)
          println("DISTANCE FOR MEDOID"+"-"*100)
          println((c+" "+cc.toString() +" " + elems+ "\n").mkString )
          println(Console.WHITE)
          */
        cc.sum / categs.size
      }
      categs.map(x => x -> sumDistance(x)).reduceLeft((a, b) => if (a._2 < b._2) a else b)
    }

    val e = Elem("centroid", newCentroid.map(_ / points.size), categs)
    if (Support.DEBUGCENTROID) {
      println(Console.YELLOW)
      println("NEW ELEMENT" + "-" * 100)
      println((e.toString() + "\n").mkString)
      println(Console.WHITE)
    }

    Some(e)
  }
}