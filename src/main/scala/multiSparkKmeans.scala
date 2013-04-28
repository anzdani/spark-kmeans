package main

import spark._
import spark.SparkContext._
import spark.util.Vector
import scala.io._
import com.codahale.jerkson.Json._

object MultiSparkKmeans {

  def main(args: Array[String]) {
    def normalize(x: Double, max: Double, min: Double, newMax: Double, newMin: Double) = 
            ((x-min)/(max-min))*(newMax-newMin)+newMin

    // Input and Parser
    /// Parse the points from a file into an RDD
    //or
    //val points2 = sc.textFile(inputFile).map(line => new Elem(line)).cache()
    def parser(input: String, sc: SparkContext): spark.RDD[Elem] = {
      val in = Source.fromFile(input).mkString.trim
      val ps = parse[List[Map[String, Any]]](in)
      
      println(Console.GREEN)
      val l: List[Elem] = ps.map(l =>
        Elem(
          List(
            Numeric("n", List(
              l.get("long").getOrElse("0").toString.toDouble,
              l.get("date").getOrElse("0=0}").toString.split("=")(1).split("}")(0).toDouble))
            ),
          List(
            IP(Set(l.get("IP").getOrElse("").toString)),
            BotName(l.get("bot").getOrElse("").toString)
            )
          )
        )

      println(l)
      println(Console.WHITE)
      sc.parallelize(l)
    }

    //TODO
    def normalizeInput(points: spark.RDD[Elem]) = points
    // system configuration parameters (from a file)
    val conf = parse[Map[String,String]](Source.fromFile("./spark.conf").mkString.trim)
    val host = conf.get("host").get.toString
    val inputFile = conf.get("inputFile").get.toString
    val appName = conf.get("appName").get.toString
    // algorithm  parameters
    val k = conf.get("initialCentroids").get.toInt
    val convergeDist = conf.get("convergeDist").get.toDouble
    
    val sc = new SparkContext(host, appName)
    val points = parser(inputFile, sc)
    println(Console.BLUE + "Read " + points.count() + " points." + Console.WHITE)
    normalizeInput(points)
   
    /* FOR DEBUG
    val ps = List(
      Elem(List(Numeric("space", List(1.0, 1.0)), Numeric("time", List(1)), IP(Set("192.169.0.1")))),
      Elem(List(Numeric("space", List(1.5, 2.0)), Numeric("time", List(2)), IP(Set("192.169.0.2")))),
      Elem(List(Numeric("space", List(3.0, 4.0)), Numeric("time", List(3)), IP(Set("192.169.0.3")))),
      Elem(List(Numeric("space", List(5.0, 7.0)), Numeric("time", List(4)), IP(Set("192.169.1.10")))),
      Elem(List(Numeric("space", List(3.5, 5.0)), Numeric("time", List(5)), IP(Set("192.169.4.50")))),
      Elem(List(Numeric("space", List(4.5, 5.0)), Numeric("time", List(6)), IP(Set("192.169.6.100")))),
      Elem(List(Numeric("space", List(3.5, 4.5)), Numeric("time", List(7)), IP(Set("192.169.10.200")))),
      Elem(List(Numeric("space", List(4.5, 5.0)), Numeric("time", List(8)), IP(Set("192.169.5.10")))))
    val points2 = sc.parallelize(ps)
    */
    //val centroids : Seq[Elem] = points.takeSample(withReplacement = false, num = k, seed = 42)
    val s = points.collect
    val centroids = List(s(0), s(1), s(6))

    // Start the Spark run
    val resultCentroids = kmeans(points, centroids, convergeDist, VSpace1())
    println(Console.GREEN)
    println(resultCentroids.map(_.toString() + "\n").mkString)
    println(Console.WHITE)
  }

   //TODO: tailrec version
  def kmeans[T : ClassManifest](points: spark.RDD[T], centroids: Seq[T], epsilon: Double, g: VectorSpace[T]): Iterable[T] = {

    def closestCentroid(centroids: Seq[T], point: T) = {
      centroids.reduceLeft(
        //search for min distance
        (a, b) => if ( g.distance(point,a)  < g.distance(point,b) ) a else b)
    }

    // Assignnment Step
    val centroidAndPoint : spark.RDD[ (T,T) ]= points.map(p => (closestCentroid(centroids, p), p))
    val clusters = centroidAndPoint.groupByKey()
    
    // Update Step
    val centers = clusters.mapValues(ps => g.centroid(ps))
    //key is the oldCentroid and value is the new one just computed

    val movement = centers.map({ case (k, v) =>  g.distance(k,v) })

    if (movement.filter(_ > epsilon).count() != 0)
      kmeans(points, centers.values.collect(), epsilon, g)
    else
      centers.values.collect() 
  }
}

trait VectorSpace[T] {
  def distance(x: T, y: T) : Double
  def centroid(ps: Seq[T]) : T
} 

@serializable case class VSpace1 extends VectorSpace[Elem]{
  
  def distanceOnFeature(f1: Feature, f2: Feature) : Double = (f1,f2) match {
      case (BotName(s1), BotName(s2)) => Levenshtein.distance(s1, s2)
      case (n1:Numeric, n2:Numeric) => (n1,n2) match {
        case _ if n1.typeName =="n" => Distance("euclidean")(n1,n2)
        case _ if n1.typeName =="space" => Distance("euclidean")(n1,n2)
        case _ if n1.typeName =="time" => Distance("euclidean")(n1,n2)
      }
      case (ip1: IP, ip2: IP) => 1 - ip1.similarity(ip2)
      case _ => 0.0
    }

    def distance(el1: Elem, el2: Elem) : Double = {
        (el1.terms, el2.terms).zipped.map(distanceOnFeature(_,_)).sum +
        (el1.categs, el2.categs).zipped.map( distanceOnFeature(_,_)).sum 
      }
    

    def centroid(c: Seq[Elem]): Elem = { 
      //compute new centroid for numeric part
      val seqTerms : Seq[ Seq[Numeric] ] = c.map(x=>x.terms)
      val newCentroid = seqTerms.reduce( (a,b) => (a, b).zipped.map(_ + _))
      
      println(Console.CYAN)

      //compute new centroid for categorical part
      var categs : Seq[Categorical] = List()
      for( i <- c(0).categs.size-1 to 0 by - 1) {
       val seqCategs : Seq[Categorical] = c.map( x=>x.categs(i) ) 
         //call newMedoid on seq of Categorical of the same type
       val cat : Categorical = newMedoid(seqCategs)._1
        //pre-append element to avoid to traverse all list
        categs = cat +: categs      
        println(cat)
        println(seqCategs)
        println(categs)
      }

      def sumDistance( c: Categorical, elems : Seq[Categorical]) : Double = {
         val cc = elems.map( distanceOnFeature(_,c ))
         //println(Console.CYAN)
         //println((c+" "+cc.toString() +" " + elems+ "\n").mkString )
         //println(Console.WHITE)
         cc.sum/elems.size
      }
      
      //compute Medoid on a sequence of categorical of the same type
      def newMedoid(categs: Seq[Categorical]) : (Categorical, Double) = 
        categs.map( x => x -> sumDistance(x,categs)).reduceLeft( (a, b) => if (a._2 < b._2) a else b )
      
      val e = Elem( newCentroid.map( _ /c.size) , categs) 
      
      println(Console.YELLOW)
      println((e.toString() + "\n").mkString )
      println(Console.WHITE)
      e
    }
}