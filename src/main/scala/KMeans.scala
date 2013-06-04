package main

import spark._
import spark.SparkContext._
import spark.broadcast.Broadcast

import scala.annotation.tailrec
import main.support.VectorSpace
/**
 * Kmeans algorithm for a generic type T in Spark
 */
object KMeans {
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
  def apply[T: ClassManifest](sc: SparkContext, points: RDD[T], centroids: Broadcast[Array[T]],
    epsilon: Double, iter: Int, maxIter: Int, vsBC: Broadcast[VectorSpace[T]]): (Seq[T], Int) = {
    val vs = vsBC.value

    def closestCentroid(point: T) = {
      centroids.value.reduceLeft(
        //search for min distance
        (a, b) => if (vs.distance(point, a) < vs.distance(point, b)) a else b)
    }

    println(Console.WHITE + "Iter:\t" + iter)
    // Assignnment Step
    //RDD Transformation to compute the list [closestCentroid, point]
    val centroidAndPoint: RDD[(T, T)] = points.map(p => (closestCentroid(p), p))

    // if (DEBUG) {
    //   println(Console.RED)
    //   println(iter + " ClosestCentroid - Point " + "-" * 100)
    //   println(centroidAndPoint.collect().map((x) => x._1.toString() + "\t-->\t" + x._2.toString() + "\n").mkString)
    //   println(Console.WHITE)
    // }

    // RDD Transformation to group cluster with result [centroid, [point]]
    val clusters: RDD[(T, Seq[T])] = centroidAndPoint.groupByKey(100)

    // if (DEBUG) {
    //   println(Console.MAGENTA)
    //   println(iter + " Clusters" + "-" * 100)
    //   println(clusters.collect().map((x) => "\nCentroid:\t" + x._1.toString() + "\nGroup:\t" + x._2.toString() + "\n").mkString)
    //   println(Console.WHITE)
    // }

    // Update Step
    // RDD Transformation to map every centroid with the new one
    val centers: RDD[(T, T)] = clusters.mapValues(ps => vs.centroid(ps).get)
    //key is the oldCentroid and value is the new one just computed

    // if (DEBUG) {
    //   println(Console.GREEN)
    //   println(iter + " OLD and NEW " + "-" * 100)
    //   println(Console.GREEN + centers.collect().map(_.toString() + "\n").mkString)
    //   println(Console.WHITE)
    // }

    // Compute the centroid movement
    // RDD Transformation to compute movement
    val movement: RDD[Double] = centers.map({ case (oldC, newC) => vs.distance(oldC, newC) })
    
    // if (DEBUG) {
    //   println(Console.YELLOW)
    //   println(iter + " movement" + "-" * 100)
    //   println(movement.collect().map("%3f".format(_).toString() + " ").mkString)
    //   println(Console.WHITE)
    // }

    val newCentroids = centers.values.collect()
    val newCentroidsBC = sc.broadcast(newCentroids)
    // Stopping condition
    // RDD Transformation to filter on a threshold value  
    if ((movement.filter(_ > epsilon).count() != 0) && iter < maxIter)
      apply(sc, points, newCentroidsBC, epsilon, iter + 1, maxIter, vsBC)
    else
      (newCentroids, iter)
  }
}
