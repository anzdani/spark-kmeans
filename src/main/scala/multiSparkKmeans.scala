package main

import spark._
import spark.SparkContext._
import spark.util.Vector

object MultiSparkKmeans {

  def main(args: Array[String]) {
    // system configuration parameters (from a file)
    //val host = Source.fromFile("").mkString.trim
    val host = "local[2]"

    val sc = new SparkContext(master = host, appName = "SparkKmeans")
    val inputFile = "./input3.txt"

    // algorithm  parameters
    val k = 3
    val convergeDist = 0.1

    // Input and Parser
    /// Parse the points from a file into an RDD
    //val points2 = sc.textFile(inputFile).map(line => new Elem(line)).cache()

    //Configuration of algorithm
    //val conf = Conf(distance=Map("space"->Distance.euclideanDistance, "time"->Distance.euclideanDistance), weights= Map("space"->0.8, "time"->0.2))

    val ps = List(
      Elem(List(Numeric("space", List(1.0, 1.0)), Numeric("time", List(1)), IP(Set("192.169.0.1")))),
      Elem(List(Numeric("space", List(1.5, 2.0)), Numeric("time", List(2)), IP(Set("192.169.0.2")))),
      Elem(List(Numeric("space", List(3.0, 4.0)), Numeric("time", List(3)), IP(Set("192.169.0.3")))),
      Elem(List(Numeric("space", List(5.0, 7.0)), Numeric("time", List(4)), IP(Set("192.169.1.1")))),
      Elem(List(Numeric("space", List(3.5, 5.0)), Numeric("time", List(5)), IP(Set("192.169.1.2")))),
      Elem(List(Numeric("space", List(4.5, 5.0)), Numeric("time", List(6)), IP(Set("192.169.2.1")))),
      Elem(List(Numeric("space", List(3.5, 4.5)), Numeric("time", List(7)), IP(Set("192.169.2.2")))))

    val points = sc.parallelize(ps)
    //val points = sc.textFile(inputFile).map( )
    println(Console.BLUE + "Read " + points.count() + " points." + Console.WHITE)
    // Initialization

    //val centroids = points.takeSample(withReplacement = false, num = k, seed = 42)
    val centroids = List(ps(0), ps(2), ps(6))

    // Start the Spark run
    val resultCentroids = kmeans(points, centroids, convergeDist)
    println(Console.YELLOW)
    println(resultCentroids.map(_.toString() + "\n").mkString)
    println(Console.WHITE)
  }

  def kmeans(points: spark.RDD[Elem], centroids: Seq[Elem], epsilon: Double): Iterable[Elem] = {
    //TODO: tailrec version

    def centroid(c: Seq[Elem]): Elem = c.reduce(_ + _) / c.length

    def closestCentroid(centroids: Seq[Elem], point: Elem) = {
      centroids.reduceLeft(
        //search for min distance
        (a, b) => if ((point distance a) < (point distance b)) a else b)
    }

    // Assignnment Step
    //the first map computes for each point the closest centroid, then emits pair (point,1), we have so <cc,[(p,1)]>
    //then a distributed reduction computes partial sums
    //finally a map computes for each centroid the new centroid
    //the result is a Map(k,v) with k oldCentroid and v newCentroid 

    val centroidAndPoint = points.map(p => (closestCentroid(centroids, p), p))
    val clusters = centroidAndPoint.groupByKey()
    val centers = clusters.mapValues(ps => centroid(ps))
    //key is the oldCentroid and value is the new one just computed

    /*
        val clusters = points.map(
          point => closestCentroid(centroids, point) -> (point, 1))
        .reduceByKeyToDriver({
          case ((ptA, numA), (ptB, numB)) => (ptA + ptB, numA + numB)
        }).map({
          case (centroid, (ptSum, numPts)) => centroid -> ptSum / numPts
        })
    */

    if (false) {
      val olds = centers.keys
      val news = centers.values
      println(Console.BLUE + "" + olds.collect() + Console.WHITE)
      println(Console.GREEN + "" + news.collect() + Console.WHITE)
    }
    val movement = centers.map({ case (k, v) => (k distance v) })

    if (movement.filter(_ > epsilon).count() != 0)
      kmeans(points, centers.values.collect(), epsilon)
    else
      centers.values.collect() // Update Step

    /*
        val newCentroids = centroids.map(oldCentroid => {
            clusters.get(oldCentroid) match {
            case Some(newCentroid) => newCentroid
            case None => oldCentroid
            }
        })
    */
    /*
        //Stopping condition
        // Calculate the centroid movement      
        val movement = (centroids zip newCentroids).map({
          case (oldCentroid, newCentroid) => oldCentroid distance newCentroid
        })
        println("Centroids changed by\n" + "\t   " + movement.map(d => "%3f".format(d)).mkString("(", ", ", ")") + "\n" + "\tto " + newCentroids.mkString("(", ", ", ")"))

        // Iterate if movement exceeds threshold
        if (movement.exists(_ > epsilon)) kmeans(points, newCentroids, epsilon)
        else return newCentroids
     */
  }

}
//case class Conf(val distance: Map[String, (Numeric,Numeric) => Double], val weights : Map[String, Double])