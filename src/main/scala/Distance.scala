package main
import scala.math
/**
 * A trait to represent a distance function
 */
trait Distance extends ((Numeric, Numeric) => Double)

/**
 * A companion object to the Distance trait to select the
 * Distance corresponding to each string description
 */
object Distance {
  def apply(description: String) = description match {
    case "cosine" => CosineDistance
    case "manhattan" => ManhattanDistance
    case "euclidean" => EuclideanDistance
    case _ => throw new MatchError("Invalid distance function: " + description)
  }
}

/**
 * Compute Cosine distance. It is obtained by subtraction the cosine similarity from one.
 */
object CosineDistance extends Distance {
  def apply(x: Numeric, y: Numeric) = 1 - x.dotProduct(y) / (x.norm * y.norm)
}

/**
 * Compute Manhattan distance (city-block)
 */
object ManhattanDistance extends Distance {
  def apply(x: Numeric, y: Numeric) = (x - y).abs.sum
}

/**
 * Compute the Euclidean distance
 */
object EuclideanDistance extends Distance {
  def apply(x: Numeric, y: Numeric) = (x - y).norm
}


object Levenshtein {
   def minimum(i1: Int, i2: Int, i3: Int)= math.min(math.min(i1, i2), i3)
   def distance(s1:String, s2:String)={
      val dist=Array.tabulate(s2.length+1, s1.length+1){(j,i)=>if(j==0) i else if (i==0) j else 0}
 
      for(j<-1 to s2.length; i<-1 to s1.length)
         dist(j)(i)=if(s2(j-1)==s1(i-1)) dist(j-1)(i-1)
	            else minimum(dist(j-1)(i)+1, dist(j)(i-1)+1, dist(j-1)(i-1)+1)
 
      dist(s2.length)(s1.length)
   }
 }
