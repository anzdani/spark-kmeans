package main.support

import spark._
import spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.Map
import main.feature.Elem
import main.support.Support._

object Evaluation {
  //TODO: it should be offline from KMeans 
  //It requires also distances and weights
  Logger.getLogger("spark").setLevel(Level.WARN)
  def apply(points: RDD[Elem], centroids: Seq[Elem], vs: VectorSpace[Elem], elMax: Elem, elMin: Elem) = {
    // Compute closest centroid given a point
    def closestCentroid(point: Elem) = {
      centroids.reduceLeft(
        //search for min distance
        (a, b) => if (vs.distance(point, a) < vs.distance(point, b)) a else b)
    }

    val centroidAndPoint: RDD[(Elem, (Double, Int))] = points.map(p => {
      val c = closestCentroid(p);
      c -> (vs.distance(p, c), 1)
    })
    //val pointGroups : RDD[(Elem, Seq[Elem])]  = points.groupBy(closestCentroid(_))

    //Compute inter class similarity between centroids
    val inter = interClassSimilarity(centroids, vs)

    //Compute intra class similarity between points
    //val intra : RDD[(Elem, (Double, Int))]= pointGroups.map(x => (x._1, intraClassSimilarity(x._1, x._2, vs)))
    //val intraValues : Array[(Elem, (Double, Int))]  = intra.collect()
    val intra: Map[Elem, (Double, Int)] = (centroidAndPoint
      .reduceByKeyToDriver({
        case ((distA, numA), (distB, numB)) =>
          (distA + distB, numA + numB)
      }))
    val intraValues: Map[Elem, (Double, Int)] = intra.map({
      case (c, (sumDist, numPts)) => (c, (sumDist / numPts, numPts))
    })

    //Only to verify number of points
    val totPoints = intraValues.map(_._2._2).reduce(_ + _)
    val intraDists = intraValues.map(_._2._1)
    val cnts = intraValues.map(_._2._2)

    val intraAvgDist = (intraDists, cnts).zipped.map(_ * _).reduce(_ + _) / totPoints

    //Quality output
    println(Console.MAGENTA)
    println("Begin QUALITY " + "-" * 100)
    println("Num of centroids:\t" + centroids.size)
    println("Num of points:\t" + totPoints)
    println("Intra Avg Distance:\t%3f".format(intraAvgDist))
    println("Num\t->\tAvg Radius")
    println(intraValues.map(x =>
      scaleElem(x._1, elMax, elMin) + "\n%d\t->\t%3f\n".format(x._2._2, x._2._1).toString())
      .mkString)
    println("Inter Avg Distance\t%3f".format(inter))
    println("End QUALITY " + "-" * 100)
    println(Console.WHITE)

  }

  //intra-class similarity
  //average distance between each point and its centers
  def intraClassSimilarity(c: Elem, points: Seq[Elem], g: VectorSpace[Elem]): (Double, Int) = {
    (
      points.map(g.distance(c, _)).sum / points.size,
      points.size)
  }

  //inter-class similarity
  //average distance between cluster centers
  //TODO: weights according to num of points in clusters
  def interClassSimilarity(centers: Seq[Elem], g: VectorSpace[Elem]): Double = {
    val n = centers.size;
    val distances: Seq[Double] = for {
      i <- 0 to n - 1
      j <- 0 to n - 1
      if j > i
    } yield (g.distance(centers(i), centers(j)))
    distances.sum / distances.size
  }
}