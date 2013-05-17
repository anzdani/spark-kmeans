package main

import spark._
import spark.SparkContext._

import scala.annotation.tailrec
import main.support.VectorSpace
/**
 * Kmeans algorithm for a generic type T in Spark
 */
object KMeans{
  var DebugCNT = 0
  val DEBUG = false

  /** 
   * Recursive function to compute centroids
   * Stopping condition: movement between old and new centroids is less than a threshold 
   * @param points    a Spark RDD of a generic type T
   * @param centroids a sequence of T elements 
   * @param epsilon   a threshold value for stopping condition
   * @param vs        a vector space instance with defined distances for features 
   *
   * @return          an iterable list of elements
   * 
   * Aim: work on distributed collections with functional operators, the same way you do for local ones
   */
  @tailrec
  def apply[T: ClassManifest](points: RDD[T], centroids: Seq[T], epsilon: Double, vs: VectorSpace[T]): Iterable[T] = {
    def closestCentroid(point: T) = {
      centroids.reduceLeft(
        //search for min distance
        (a, b) => if (vs.distance(point, a) < vs.distance(point, b)) a else b)
    }

    println(Console.WHITE+"Iter:\t" + DebugCNT)
    DebugCNT += 1
    
    // Assignnment Step
    //RDD Transformation to compute the list [closestCentroid, point]
    val centroidAndPoint: RDD[(T, T)] = points.map(p => (closestCentroid(p), p))
    if (DEBUG){
      println(Console.RED)
      println(DebugCNT + " ClosestCentroid - Point " + "-" * 100)
      println(centroidAndPoint.collect().map((x) => x._1.toString() + "\t-->\t" + x._2.toString() + "\n").mkString)
      println(Console.WHITE)
    }

    // RDD Transformation to group cluster with result [centroid, [point]]
    val clusters: RDD[(T, Seq[T])] = centroidAndPoint.groupByKey()
    if (DEBUG){
      println(Console.MAGENTA)
      println(DebugCNT + " Clusters" + "-" * 100)
      println(clusters.collect().map((x) => "\nCentroid:\t" + x._1.toString() + "\nGroup:\t" + x._2.toString() + "\n").mkString)
      println(Console.WHITE)
    }
    
    // Update Step
    // RDD Transformation to map every centroid with the new one
    val centers : RDD[(T, T)] = clusters.mapValues(ps => vs.centroid(ps).get)
    //key is the oldCentroid and value is the new one just computed
    if (DEBUG){
      println(Console.GREEN)
      println(DebugCNT + " OLD and NEW " + "-" * 100)
      println(Console.GREEN + centers.collect().map(_.toString() + "\n").mkString)
      println(Console.WHITE)
    }
    
    // Compute the centroid movement
    // RDD Transformation to compute movement
    val movement : RDD[Double] = centers.map({ case (oldC, newC) => vs.distance(oldC, newC) })
    if (DEBUG){
      println(Console.YELLOW)
      println(DebugCNT + " movement" + "-" * 100)
      println(movement.collect().map("%3f".format(_).toString() + " ").mkString) 
      println(Console.WHITE)
    }

    // Stopping condition
    // RDD Transformation to filter on a threshold value  
    if (movement.filter(_ > epsilon).count() != 0)
      apply(points, centers.values.collect(), epsilon, vs)
    else
      centers.values.collect()
  }
}
