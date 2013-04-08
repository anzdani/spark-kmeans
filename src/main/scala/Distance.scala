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

