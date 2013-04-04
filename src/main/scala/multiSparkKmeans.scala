
import spark._
import spark.SparkContext._
import spark.util.Vector

object MultiSparkKmeans {
  
  def main(args: Array[String]) {
        // system configuration parameters
        //val host = Source.fromFile("").mkString.trim
        val host = "local[2]"

        val sc = new SparkContext(master = host, appName = "SparkKmeans")
        val inputFile = "/Users/dani/spark-kmeans/input3.txt"

        // algorithm  parameters
        val k = 3
        val convergeDist = 0.1

        // Input and Parser
        /// Parse the points from a file into an RDD
        val points2 = sc.textFile(inputFile).map(line => new Elem(line)).cache()
        val ps = List ( Elem(  List(Space(1.0, 1.0), Time(1), IP(Set("192.169.0.1"))) ),
                        Elem(  List(Space(1.5, 2.0), Time(2), IP(Set("192.169.0.2"))) ),
                        Elem(  List(Space(3.0, 4.0), Time(3), IP(Set("192.169.0.3"))) ),
                        Elem(  List(Space(5.0, 7.0), Time(4), IP(Set("192.169.1.1"))) ),
                        Elem(  List(Space(3.5, 5.0), Time(5), IP(Set("192.169.1.2"))) ),
                        Elem(  List(Space(4.5, 5.0), Time(6), IP(Set("192.169.2.1"))) ),
                        Elem(  List(Space(3.5, 4.5), Time(7), IP(Set("192.169.2.2"))) )
                    )

        val points = sc.parallelize(ps)
        //val points = sc.textFile(inputFile).map( )
        println(Console.BLUE+"Read " + points.count() + " points."+Console.WHITE)
        // Initialization
        
        //val centroids = points.takeSample(withReplacement = false, num = k, seed = 42)
        val centroids = List(ps(0), ps(2), ps(6))

        // Start the Spark run
        val resultCentroids = kmeans(points, centroids, convergeDist)
        println(Console.YELLOW)
        println(resultCentroids.map(_.toString()+"\n").mkString)
         println(Console.WHITE)
    }


  def kmeans(points: spark.RDD[Elem],centroids: Seq[Elem], epsilon: Double): Iterable[Elem] = {
    //TODO: tailrec version
      def centroid(c: Seq[Elem]): Elem = c.reduce(_+_)/c.length

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
        
        val centroidAndPoint = points.map( p =>  (closestCentroid(centroids,p),p) )
        val clusters = centroidAndPoint.groupByKey()
        
        val centers = clusters.mapValues( ps => centroid(ps))
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

        if (true) {
          val olds = centers.keys
          val news = centers.values
          println(Console.BLUE+""+olds.collect()+Console.WHITE)
          println(Console.GREEN+""+news.collect()+Console.WHITE)
        }

        val movement = centers.map( { case (k, v) => (k distance v)} ) 
        
        if ( movement.filter(_ > epsilon).count()!=0)  
            kmeans(points, centers.values.collect(), epsilon)
        else 
            centers.values.collect()        // Update Step
        
        /*
        val newCentroids = centroids.map(oldCentroid => {
            newCenters.get(oldCentroid) match {
            case Some(newCentroid) => newCentroid
            case None => oldCentroid
            }
        })
        */


        //Stopping condition
        // Calculate the centroid movement
        /*
        val movement = (centroids zip newCentroids).map({
          case (oldCentroid, newCentroid) => oldCentroid distance newCentroid
        })
        val movement = newCenters.
        println("Centroids changed by\n" + "\t   " + movement.map(d => "%3f".format(d)).mkString("(", ", ", ")") + "\n" + "\tto " + newCentroids.mkString("(", ", ", ")"))

        // Iterate if movement exceeds threshold
        if (movement.exists(_ > epsilon)) kmeans(points, newCentroids, epsilon)
        else return newCentroids
      */
    }
}

/*
trait Geometry[T] {
    def distance(x: T, y: T): Double
    def centroid(c: Seq[T]): T

    def closestCentroid(centroids: Seq[T], point: T): T = {
        centroids.reduceLeft(
        //search for min distance
        (a, b) =>
        if (distance(point, a) < distance(point, b)) a
        else b)
    }
/*
  def nearest(cs: Seq[T], p: T): Int =
    cs.map(distance(p,_)).zipWithIndex.min._2
  */
}
*/

trait Feature {

    def distance(that: Feature): Double = (this, that) match {
      case (Space(lat1, long1), Space(lat2, long2)) => math.sqrt(math.pow(lat1 - lat2, 2) + math.pow(long1 - long2, 2))
      case (Time(t1), Time(t2)) => math.abs(t1-t2)
      case (IP(ip1), IP(ip2)) => 0.0
      case _ => 0.0
    }

    def + (that: Feature): Feature = (this, that) match {
      case (Space(lat1, long1), Space(lat2, long2)) => Space(lat1 + lat2, long1 + long2)
      case (Time(t1), Time(t2)) => Time(t1 + t2)
      case (IP(ip1), IP(ip2)) => IP(ip1++ip2) //
    
    }

    def / (num: Int) = this match {
      case _ if num == 0 => this
      case Space(lat, long) => Space(lat/num, long/num)
      case Time(t) => Time(t/num)
      case IP(ip) =>  this 
    }
}

case class Space(val lat: Double, val long: Double) extends Feature
case class Time(val t: Double) extends Feature
case class IP(val ip: Set[String]) extends Feature

case class Elem(val terms: Seq[Feature]) {

    def this(s: String) = this(List(Space(0, 1), Time(1), IP(Set("192.169.0.1"))))

    def + (that: Elem) = Elem((that.terms, this.terms).zipped.map(_ + _))

    def / (num: Int) : Elem = Elem( this.terms.map( _ / num))

    def distance(that: Elem) : Double = (this.terms, that.terms).zipped.map( _ distance _ ).sum

}

/*
@serializable case class Point(val nterms: Map[String, Double], val sterms: Map[String, String]) {
  
  def this(bindings: (String, Double)*) = this(bindings.toMap)
  val nterms = terms0 withDefaultValue 0.0

  //TODO: lista e indici
  //caso generale: mail diverse simile a polinomio
  def +(that: Point) =  Point((that.terms foldLeft terms)(addTerm))
  
  def addTerm(terms: Map[Int, Double], term: (Int, Double)): Map[Int, Double] = {
    val (key, value) = term
    terms + (key -> (value + terms(key)))
  }
  
  override def toString = {
    (for ((key, value) <- terms.toList.sorted.reverse) yield key +" " + value) mkString " + "
  }
  
  //def -(that: Point) = this + (-that)
  //def unary_-() = Point(-this.x, -this.y)
  def /(d: Double) = new Point(terms map( { case (k,v) => k->(v/d)} ))
  //new Point(terms map( pair =>  pair match { case (k,v) => k->(v/d)} ))
  
  
  //to define according to distance (Euclidean distance)
  //def magnitude = math.sqrt(x * x + y * y)
  //def distance(that: Point) = (that - this).magnitude
  
}
*/

/*
  //def -(that: Point) = this + (-that
  //def unary_-() = Point(-this.x, -this.y)
  def /(d: Double) = Elem ( this.nterm.map(_/d), this.sterm)
  //new Point(terms map( pair =>  pair match { case (k,v) => k->(v/d)} ))
  
  def distance(that: Elem) = 0.0
  //to define according to distance (Euclidean distance)
  //def magnitude = math.sqrt(x * x + y * y)
  //def distance(that: Point) = (that - this).magnitude
  
}

*/