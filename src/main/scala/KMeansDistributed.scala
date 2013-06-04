package main

import spark._
import spark.SparkContext._
import spark.broadcast.Broadcast

import scala.annotation.tailrec
import main.support.VectorSpace
import scala.collection.Map
/**
 * Kmeans algorithm for a generic type T in Spark
 */
object KMeansDistributed{
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
    epsilon: Double, iter: Int, maxIter: Int, vsBC: Broadcast[VectorSpace[T]]): (Seq[T],Int) = {
    
    val vs = vsBC.value
    def closestCentroid(point: T) = {
      centroids.value.reduceLeft(
        //search for min distance
        (a, b) => if (vs.distance(point, a) < vs.distance(point, b)) a else b)
    }

    println(Console.WHITE+"Iter:\t" + iter)
    
    // Assignnment Step
    //RDD Transformation to compute the list [closestCentroid, (point,1)]
    val centroidAndPoint : RDD[(T, (T, Int))]  = points.map(p => closestCentroid(p) -> (p,1))
    
    // Update Step
    val centers  : Map[T, T] = (centroidAndPoint
      .reduceByKeyToDriver({
         case ((elemA, numA), (elemB, numB)) => 
            (vs.mergeElems( elemA, elemB), numA + numB)
      })
      .map({
         case (centroid, (ptSum, numPts)) => centroid -> vs.divideElem(ptSum,numPts)
      })
    )

    // Compute the centroid movement
    // RDD Transformation to compute movement
    val movement : Iterable[Double] = centers.map({ case (oldC, newC) => vs.distance(oldC, newC) })
    
    val newCentroids = centers.values.toArray
    val newCentroidsBC = sc.broadcast(newCentroids)
    
    // Stopping condition
    // RDD Transformation to filter on a threshold value  
    if ((movement.exists(_ > epsilon)) && iter < maxIter)
      apply(sc,points, newCentroidsBC, epsilon, iter+1, maxIter, vsBC)
    else
      (newCentroids,iter)
  }
}