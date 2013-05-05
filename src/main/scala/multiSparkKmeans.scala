package main

import spark._
import spark.SparkContext._
import spark.util.Vector
import scala.io._
import com.codahale.jerkson.Json._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {
  Logger.getLogger("spark").setLevel(Level.WARN)
  def main(args: Array[String]) {
    MultiSparkKmeans.run("./spark.conf")
  }

  var DebugCNT = 0
  val DEBUGCENTROID = false
}

object MultiSparkKmeans {
  // Input and Parser
  def parser(input: String, sc: SparkContext): RDD[Elem] = {
    val data = sc.textFile(input)
    val featurized: RDD[Elem] = data.map(line => {
      val l = parse[Map[String, Any]](line)
      Elem(
        List(
          Numeric("numeric", List(
            l.get("long").getOrElse("0").toString.toDouble,
            l.get("lat").getOrElse("0").toString.toDouble,
            l.get("date").getOrElse("0=0}").toString.split("=")(1).split("}")(0).toDouble))),
        List(
          IP(Set(l.get("IP").getOrElse("").toString)),
          BotName(l.get("bot").getOrElse("").toString)))

    })
    //Update weights for Numeric type
    featurized
  }

  //TODO: improve
  def normalizeInput(points: RDD[Elem]) : RDD[Elem] = {
    val elMax : Elem = points.reduce( 
      (a:Elem, b:Elem) => {  
        Elem( (a.terms, b.terms).zipped.map( _ maxLimit _ ),
          (a.categs, b.categs).zipped.map( _ maxLimit _ ) )
    })

    val elMin = points.reduce( 
      (a:Elem, b:Elem) => {  
        Elem( (a.terms, b.terms).zipped.map( _ minLimit _ ),
          (a.categs, b.categs).zipped.map( _ minLimit _ ) )
    })

    println(Console.YELLOW)
    println("Max-Min"+"-"*100)
    println(elMax)
    println(elMin)

    def normalizeNumeric(t: Numeric, max: Numeric, min:Numeric) : Numeric = {
    Numeric(t.typeName,
      for {
        i <- 0 to t.terms.size-1; newTerms = Normalize(t.terms(i), max.terms(i), min.terms(i), 1, 0)
      }yield( newTerms))
    }

    points.map( el => { 
        val newNterms : Seq[Numeric] = for {
          i <- 0 to el.terms.size-1; nterms = normalizeNumeric(el.terms(i),elMax.terms(i), elMin.terms(i))
          } yield(nterms)
      Elem(newNterms,el.categs)
      }
    )
  }

  
  // system configuration parameters (from a file)

  def run(config : String ) = {
    val conf = parse[Map[String, String]](Source.fromFile(config).mkString.trim)
    val host = conf.get("host").get.toString
    val inputFile = conf.get("inputFile").get.toString
    val appName = conf.get("appName").get.toString
    // algorithm  parameters
    val k = conf.get("initialCentroids").get.toInt
    val convergeDist = conf.get("convergeDist").get.toDouble

    val sc = new SparkContext(host, appName)

    val pointsRaw = parser(inputFile, sc)
    Numeric.weights = Map("numeric" -> List(0.5,0.2,0.3) )

    println(Console.BLUE)
    println("READ"+"-"*100)
    println( pointsRaw.collect().map(_.toString() + "\n").mkString)

    println(Console.BLUE + "Read " + pointsRaw.count() + " points." + Console.WHITE)
    val points = normalizeInput(pointsRaw)

    //val centroids : Seq[Elem] = points.takeSample(withReplacement = false, num = k, seed = 42)
    val s = points.collect
    val centroids = List(s(0), s(1), s(6))

    // Start the Spark run
    val weights = List(0.5, 0.2, 0.3)

    val resultCentroids = kmeans(points, centroids, convergeDist, VSpace(weights))
    
    println(Console.BLUE)
    println("RESULT"+"-"*100)
    println(resultCentroids.map(_.toString() + "\n").mkString)
    println(Console.WHITE)
  }

  //TODO: tailrec version
  def kmeans[T: ClassManifest](points: RDD[T], centroids: Seq[T], epsilon: Double, g: VectorSpace[T]): Iterable[T] = {

    def closestCentroid(centroids: Seq[T], point: T) = {
      centroids.reduceLeft(
        //search for min distance
        (a, b) => if (g.distance(point, a) < g.distance(point, b)) a else b)
    }

    Main.DebugCNT +=1
    // Assignnment Step
    val centroidAndPoint: RDD[(T, T)] = points.map(p => (closestCentroid(centroids, p), p))
    println(Console.RED)
    println(Main.DebugCNT+" ClosestCentroid - Point "+"-"*100)
    println(centroidAndPoint.collect().map( (x) => x._1.toString() +"\t-->\t"+ x._2.toString() + "\n").mkString )
    println(Console.WHITE)
    
    val clusters = centroidAndPoint.groupByKey()
    println(Console.MAGENTA)
    println(Main.DebugCNT+" Clusters"+"-"*100)
    println(clusters.collect().map((x) => "\nCentroid:\t"+x._1.toString()+"\nGroup:\t"+ x._2.toString()+ "\n").mkString )
    println(Console.WHITE)
    
    // Update Step
    val centers = clusters.mapValues(ps => g.centroid(ps))
    //key is the oldCentroid and value is the new one just computed
    println(Console.GREEN)
    println(Main.DebugCNT+" OLD and NEW "+"-"*100)
    println(Console.GREEN+centers.collect().map(_.toString() + "\n").mkString )
    println(Console.WHITE)
    
    val movement = centers.map({ case (k, v) => g.distance(k, v) })
    println(Console.BLUE)
    println(Main.DebugCNT+" movement"+"-"*100)
    println(Console.BLUE+movement.collect().map(_.toString() + "\n").mkString )
    println(Console.WHITE)
    
    if (movement.filter(_ > epsilon).count() != 0)
      kmeans(points, centers.values.collect(), epsilon, g)
    else
      centers.values.collect()
  }

}

object Normalize{
  def apply(x: Double, max: Double, min: Double, newMax: Double, newMin: Double) =
    ((x - min) / (max - min)) * (newMax - newMin) + newMin
}

trait VectorSpace[T] {
  def distance(x: T, y: T): Double
  def centroid(ps: Seq[T]): T
}

@serializable case class VSpace(val weights : List[Double]) extends VectorSpace[Elem] {
  
  def distanceOnFeature(f1: Feature, f2: Feature): Double = (f1, f2) match {
    case (BotName(s1), BotName(s2)) => Levenshtein.distance(s1, s2)
    //1- distance(a,b)/max(a.length, b.length).
    case (n1: Numeric, n2: Numeric) => (n1, n2) match {
      case _ if n1.typeName == "numeric" => Normalize(Distance("euclidean")(n1, n2), max=math.sqrt(2), min=0, newMax=1, newMin=0)
      case _ if n1.typeName == "space" => Normalize(Distance("euclidean")(n1, n2), max=math.sqrt(2), min=0, newMax=1, newMin=0)
      case _ if n1.typeName == "time" => Normalize(Distance("euclidean")(n1, n2), max=math.sqrt(2), min=0, newMax=1, newMin=0)
    }
    case (ip1: IP, ip2: IP) => 1 - ip1.similarity(ip2)
    case _ => 0.0
  }

  def distance(el1: Elem, el2: Elem): Double = {
      val ndist : Seq[Double] = (el1.terms, el2.terms).zipped.map(distanceOnFeature(_, _))
      val cdist : Seq[Double] = (el1.categs, el2.categs).zipped.map(distanceOnFeature(_, _))
      //weighted distance
      val d = ndist++cdist
      require(d.size == weights.size, "Error: wrong number of features")
      (d, weights).zipped.map(_ * _).sum 
  }

  def centroid(c: Seq[Elem]): Elem = {
    //compute new centroid for numeric part
    val seqTerms: Seq[Seq[Numeric]] = c.map(x => x.terms)
    val newCentroid = seqTerms.reduce((a, b) => (a, b).zipped.map(_ + _))

    if (Main.DEBUGCENTROID){
      println(Console.CYAN)
      println("NEW MEDOID PART"+"-"*100)
    }
    //compute new centroid for categorical part
    var categs: Seq[Categorical] = List()
    for (i <- c(0).categs.size - 1 to 0 by -1) {
      val seqCategs: Seq[Categorical] = c.map(x => x.categs(i))
      //call newMedoid on seq of Categorical of the same type
      val cat: Categorical = newMedoid(seqCategs)._1
      //pre-append element to avoid to traverse all list
      categs = cat +: categs
      if (Main.DEBUGCENTROID){
        println("new:\t\t" + cat)
        println("seqCategs:\t" + seqCategs)
        println("add part:\t" + categs)
      }
    }
    println(Console.WHITE)

    def sumDistance(c: Categorical, elems: Seq[Categorical]): Double = {
      val cc = elems.map(distanceOnFeature(_, c))
      /*
      println(Console.GREEN)
      println("DISTANCE FOR MEDOID"+"-"*100)
      println((c+" "+cc.toString() +" " + elems+ "\n").mkString )
      println(Console.WHITE)
      */
      cc.sum / elems.size
    }

    //compute Medoid on a sequence of categorical of the same type
    def newMedoid(categs: Seq[Categorical]): (Categorical, Double) =
      categs.map(x => x -> sumDistance(x, categs)).reduceLeft((a, b) => if (a._2 < b._2) a else b)

    val e = Elem(newCentroid.map(_ / c.size), categs)
    if (Main.DEBUGCENTROID){
      println(Console.YELLOW)
      println("NEW ELEMENT"+"-"*100)
      println((e.toString() + "\n").mkString)
      println(Console.WHITE)
    }
    e
  }
}