package main

import spark._
import spark.SparkContext._
import spark.util.Vector
import scala.io._
import com.codahale.jerkson.Json._
import com.rockymadden.stringmetric
import com.rockymadden.stringmetric.similarity._
import scala.annotation.tailrec
import scala.util.Random.nextInt

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {
  Logger.getLogger("spark").setLevel(Level.WARN)
  def main(args: Array[String]) {
    MultiSparkKmeans.run("./spark.conf")
  }

  var DebugCNT = 0
  val DEBUGCENTROID = false
  val DEBUG = false
}

object MultiSparkKmeans {
  // Input and Parser
  var weights : List[Double] = List()

  def parser(input: String, sc: SparkContext): RDD[Elem] = {

    def extractFromArray(k: String, m: Map[String, Any], default: String): List[String] =
      m.get(k).getOrElse("[" + default + ",]").toString.drop(1).dropRight(1).split(",").toList

    def extractFromValue(k: String, m: Map[String, Any], default: String): String = { 
      val s = m.get(k).getOrElse(default).toString
      if (s.isEmpty) "0.0"
      else s
     } 
    //TODO: for map if many uri
    def extractFromMap(k: String, m: Map[String, Any], default: String) =
      m.get(k).getOrElse("0=" + default + "}").toString.split("=")(1).split("}")(0)

    val data = sc.textFile(input)
    val featurized: RDD[Elem] = data.map(line => {
      val m = parse[Map[String, Any]](line)

      //partial constructor
      val ip = Categorical(typeName = "IP", _: String)
      val bot = Categorical(typeName = "bot", _: String)
      val uri = Categorical(typeName = "uri", _: String)
      val n = Numeric(typeName = "numeric", _: Seq[Double])

      Elem(
        extractFromMap("_id", m, "0"),
        List(
          n(List(
            extractFromValue("long", m, "0.0").toDouble,
            extractFromValue("lat", m, "0.0").toDouble,
            extractFromMap("date", m, "0.0").toDouble))),
        List(
          ip(extractFromValue("IP", m, "")),
          bot(extractFromValue("bot", m, "")),
          uri(extractFromArray("uri", m, "")(0))))
    })
    featurized
  }

  //TODO: improve - MAX and MIN are already known or not? 
  def normalizeInput(points: RDD[Elem]): RDD[Elem] = {
    val elMax: Elem = points.reduce(
      (a: Elem, b: Elem) => {
        Elem("max", (a.terms, b.terms).zipped.map(_ maxLimit _),
          (a.categs, b.categs).zipped.map(_ maxLimit _))
      })

    val elMin = points.reduce(
      (a: Elem, b: Elem) => {
        Elem("min", (a.terms, b.terms).zipped.map(_ minLimit _),
          (a.categs, b.categs).zipped.map(_ minLimit _))
      })
    if (Main.DEBUG){
      println(Console.YELLOW)
      println("Max-Min" + "-" * 100)
      println(elMax)
      println(elMin)
    }

    def normalizeNumeric(t: Numeric, max: Numeric, min: Numeric): Numeric = {
      Numeric(t.typeName,
        for {
          i <- 0 to t.terms.size - 1; newTerms = Normalize(t.terms(i), max.terms(i), min.terms(i), 1, 0)
        } yield (newTerms))
    }

    points.map(el => {
      val newNterms: Seq[Numeric] = for {
        i <- 0 to el.terms.size - 1; nterms = normalizeNumeric(el.terms(i), elMax.terms(i), elMin.terms(i))
      } yield (nterms)
      Elem(el.id, newNterms, el.categs)
    })
  }

  def run(config: String) = {
    // system configuration parameters (from a file)
    val conf = parse[Map[String, String]](Source.fromFile(config).mkString.trim)
    val host = conf.get("host").get.toString
    val inputFile = conf.get("inputFile").get.toString
    val appName = conf.get("appName").get.toString
    // algorithm  parameters
    val k = conf.get("initialCentroids").get.toInt
    val convergeDist = conf.get("convergeDist").get.toDouble

    val sc = new SparkContext(host, appName)

    val pointsRaw = parser(inputFile, sc)
    Numeric.weights = Map("numeric" -> List(0.3, 0.3, 0.4))
    MultiSparkKmeans.weights = List(0.25, 0.25, 0.25, 0.25)

    println(Console.CYAN)
    println("READ" + "-" * 100)
    println(Console.CYAN + "Read " + pointsRaw.count() + " points." )
    
    if (Main.DEBUG){
      println(pointsRaw.collect().map(_.toString() + "\n").mkString)
      println(Console.WHITE)
    }
    val points = normalizeInput(pointsRaw)

    val centroids: Seq[Elem] = points.takeSample(withReplacement = false, num = k, seed = nextInt())
    /*
    For DEBUG
    val s = points.collect
    val centroids = List(s(2), s(7),  Elem(
        "none",
        List(
            Numeric("numeric",List(2,2,2))
        ),
        List(
          Categorical("IP","1.1.1.1"),
          Categorical("bot","w"),
          Categorical("uri","w")
        )
      ))
    */
    // Start the kmeans run
    val geometry = VSpace(weights)
    val resultCentroidAndGroups = kmeans(points, centroids, convergeDist, geometry).collect()
    //Centroids output
      println(Console.GREEN+Console.BOLD)
      println("RESULT-CENTROIDS" + "-" * 100)
      println(resultCentroidAndGroups.map(x => "\n\nCentroid:"+x._1.toString()+"\n"+"Num points:\t"+x._2.size).mkString)
      println(Console.WHITE)


    //Compute inter class similarity between centroids
    val inter = interClassSimilarity(resultCentroidAndGroups.map(_._1), geometry)
    //Compute intra class similarity between points
    val intra = resultCentroidAndGroups.map(x => intraClassSimilarity(x._1, x._2, geometry))
    //Quality output
      println(Console.MAGENTA)
      println("QUALITY " + "-" * 100)
      println("\nInter:\t"+inter+"\nIntra:\t"+intra.map("%3f".format(_).toString() + " ").mkString)
      println(Console.WHITE)
    //Clusters output
      println(Console.CYAN+Console.BOLD)
      println("RESULT-CLUSTERS" + "-" * 100)
      println(resultCentroidAndGroups.map(x => "\n\nCentroid:"+x._1.toString()+"\nCluster:\n"+ x._2.toString()+"\n").mkString)
      println(Console.WHITE)    
  }

  @tailrec
  def kmeans[T: ClassManifest](points: RDD[T], centroids: Seq[T], epsilon: Double, g: VectorSpace[T]): RDD[(T, Seq[T])] = {

    def closestCentroid(centroids: Seq[T], point: T) = {
      centroids.reduceLeft(
        //search for min distance
        (a, b) => if (g.distance(point, a) < g.distance(point, b)) a else b)
    }

    println(Console.WHITE+"Iter:\t" + Main.DebugCNT)
    Main.DebugCNT += 1
    
    // Assignnment Step
    val centroidAndPoint: RDD[(T, T)] = points.map(p => (closestCentroid(centroids, p), p))
    if (Main.DEBUG){
      println(Console.RED)
      println(Main.DebugCNT + " ClosestCentroid - Point " + "-" * 100)
      println(centroidAndPoint.collect().map((x) => x._1.toString() + "\t-->\t" + x._2.toString() + "\n").mkString)
      println(Console.WHITE)
    }

    val clusters: RDD[(T, Seq[T])] = centroidAndPoint.groupByKey()
    if (Main.DEBUG){
      println(Console.MAGENTA)
      println(Main.DebugCNT + " Clusters" + "-" * 100)
      println(clusters.collect().map((x) => "\nCentroid:\t" + x._1.toString() + "\nGroup:\t" + x._2.toString() + "\n").mkString)
      println(Console.WHITE)
    }

    // Update Step
    val centers : RDD[(T, (T, Seq[T]))] = clusters.mapValues(ps => (g.centroid(ps).get,ps))
    //key is the oldCentroid and value is the new one just computed
    if (Main.DEBUG){
      println(Console.GREEN)
      println(Main.DebugCNT + " OLD and NEW " + "-" * 100)
      println(Console.GREEN + centers.collect().map(_.toString() + "\n").mkString)
      println(Console.WHITE)
    }
    
    val newClusters = centers.values
    val newCenters = newClusters.keys.collect()
    //val oldCenters = centers.keys.collect()
    //val movement = (oldCenters,newCenters).zipped.map({ case (k, v) => g.distance(k, v) })
    
    val movement = centers.map({ case (k, (v,ps)) => g.distance(k, v) })
    
      println(Console.YELLOW)
      println(Main.DebugCNT + " movement" + "-" * 100)
      println(movement.collect().map("%3f".format(_).toString() + " ").mkString) 
      //println(newCenters.map(_.toString() +"\t").mkString)
      println(Console.WHITE)
    
    if (movement.filter(_ > epsilon).count() != 0)
      kmeans(points, newCenters, epsilon, g)
    else
      newClusters
  }

  //intra-class similarity
  //average distance between each point and its centers
  def intraClassSimilarity[T: ClassManifest](c: T, points: Seq[T], g: VectorSpace[T]) : Double = {
    points.map( g.distance(c,_)).sum/points.size
  }
  
  //inter-class similarity
  //weighted average distance between cluster centers
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

object Normalize {
  def apply(x: Double, max: Double, min: Double, newMax: Double, newMin: Double) =
    ((x - min) / (max - min)) * (newMax - newMin) + newMin
}

trait VectorSpace[T] {
  def distance(x: T, y: T): Double
  def centroid(ps: Seq[T]): Option[T]
}

@serializable case class VSpace(val weights: List[Double]) extends VectorSpace[Elem] {

  def distanceOnFeature(f1: Feature, f2: Feature): Double = (f1, f2) match {
    case (c1: Categorical, c2: Categorical) => (c1, c2) match {
      case _ if c1.typeName == "ip" => 1 - IP.similarity(c1.term, c2.term)
      case _ if c1.typeName == "bot" => {
        val d = Levenshtein.distance(c1.term, c2.term)
        val v = d / math.max(math.max(c1.term.size, c2.term.size),1)
        require(v <= 1, "Distance between 0 and 1")
        v
      }
      case _ if c1.typeName == "uri" => {
        val d = Levenshtein.distance(c1.term, c2.term)
        val v = d / math.max(math.max(c1.term.size, c2.term.size),1)
        require(v <= 1, "Distance between 0 and 1")
        v
      }
      case _ => 0.0

    }

    case (n1: Numeric, n2: Numeric) => (n1, n2) match {
      case _ if n1.typeName == "numeric" => Normalize(Distance("euclidean")(n1, n2), max = math.sqrt(2), min = 0, newMax = 1, newMin = 0)
      case _ if n1.typeName == "space" => Normalize(Distance("euclidean")(n1, n2), max = math.sqrt(2), min = 0, newMax = 1, newMin = 0)
      case _ if n1.typeName == "time" => Normalize(Distance("euclidean")(n1, n2), max = math.sqrt(2), min = 0, newMax = 1, newMin = 0)
      case _ => 0.0
    }
    case _ => 0.0
  }

  def distance(el1: Elem, el2: Elem): Double = {
    val ndist: Seq[Double] = (el1.terms, el2.terms).zipped.map(distanceOnFeature(_, _))
    val cdist: Seq[Double] = (el1.categs, el2.categs).zipped.map(distanceOnFeature(_, _))
    //weighted distance
    val d = ndist ++ cdist
    require(d.size == weights.size, "Error: wrong number of features")
    (d, weights).zipped.map(_ * _).sum
  }

  def centroid(c: Seq[Elem]): Option[Elem] = {
    //compute new centroid for numeric part
    if (c.isEmpty) return None
    val seqTerms: Seq[Seq[Numeric]] = c.map(x => x.terms)
    val newCentroid = seqTerms.reduce((a, b) => (a, b).zipped.map(_ + _))

    if (Main.DEBUGCENTROID) {
      println(Console.CYAN)
      println("NEW MEDOID PART" + "-" * 100)
    }
    //compute new centroid for categorical part
    var categs: Seq[Categorical] = List()
    for (i <- c(0).categs.size - 1 to 0 by -1) {
      val seqCategs: Seq[Categorical] = c.map(x => x.categs(i))
      //call newMedoid on seq of Categorical of the same type
      val cat: Categorical = newMedoid(seqCategs)._1
      //pre-append element to avoid to traverse all list
      categs = cat +: categs
      if (Main.DEBUGCENTROID) {
        println("new:\t\t" + cat)
        println("seqCategs:\t" + seqCategs)
        println("add part:\t" + categs)
      }
    }
    

    def sumDistance(c: Categorical, elems: Seq[Categorical]): Double = {
      if (elems.isEmpty) return 0
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

    val e = Elem("centroid", newCentroid.map(_ / c.size), categs)
    if (Main.DEBUGCENTROID) {
      println(Console.YELLOW)
      println("NEW ELEMENT" + "-" * 100)
      println((e.toString() + "\n").mkString)
      println(Console.WHITE)
    }
    Some(e)
  }
}