package main.support

import spark._
import spark.SparkContext._


object Evaluation{

  def apply[T: ClassManifest](points: RDD[T], centroids: Seq[T], vs: VectorSpace[T]) = {
    // Compute closest centroid given a point
    def closestCentroid(point: T) = {
      centroids.reduceLeft(
        //search for min distance
        (a, b) => if (vs.distance(point, a) < vs.distance(point, b)) a else b)
    }

  val pointGroups : RDD[(T, Seq[T])]  = points.groupBy(closestCentroid(_))

  //Compute inter class similarity between centroids
  val inter = interClassSimilarity(centroids, vs)
  
  //Compute intra class similarity between points
  val intra : RDD[Double]= pointGroups.map(x => intraClassSimilarity(x._1, x._2, vs))
  
  //Quality output
  println(Console.MAGENTA)
  println("QUALITY " + "-" * 100)
  println("\nInter:\t"+inter+"\nIntra:\t"+intra.collect().map("%3f".format(_).toString() + " ").mkString)
  println(Console.WHITE)
  
  }
  
  //intra-class similarity
  //average distance between each point and its centers
  def intraClassSimilarity[T: ClassManifest](c: T, points: Seq[T], g: VectorSpace[T]) : Double = {
    points.map( g.distance(c,_)).sum/points.size
  }
  
  //inter-class similarity
  //average distance between cluster centers
  //TODO: weights according to num of points in clusters
  def interClassSimilarity[T: ClassManifest](centers: Seq[T],g: VectorSpace[T]) : Double = {
    val n = centers.size;
    val distances : Seq[Double] = for { 
      i <- 0 to n-1
      j <- 0 to n-1
      if j>i
    } yield (g.distance(centers(i), centers(j)))
    distances.sum/distances.size
  }
}