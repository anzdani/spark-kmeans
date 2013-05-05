package main
import javax.naming.directory.{ InitialDirContext, Attribute }
import javax.naming.NamingException
import scala.collection.JavaConversions._

/**
 * A  representation of a point in some n-dimensional space
 *
 * @param terms  a sequence of Feature that defines this point's coordinates in some space
 *
 */
@serializable case class Elem(val terms: Seq[Numeric], val categs: Seq[Categorical]){
  override def toString = "\n[Numeric:  "+terms+"\tCategs:  "+categs+"]"
}
//it is better to separate feature in these 2 type for perfomance during the computation of medoid

trait Feature{
}

trait Categorical extends Feature{
  def minLimit(that: Categorical) : Categorical = this
  def maxLimit(that: Categorical) : Categorical = this
}

object Numeric{
  var weights : Map[String, List[Double]] = Map()
}
case class BotName(val name : String) extends Categorical{
  def minLimit(that:BotName) = if (this.name.size < that.name.size) this else that 
  def maxLimit(that:BotName) = if (this.name.size > that.name.size) this else that 
}


//TODO: factory pattern
/**
 * A feature to represent a numeric field that represent a MultiPoint in some space
 * @param typeName a name to identify the feature
 * @param terms a sequence of coordinates
 */
case class Numeric(val typeName: String, val terms: Seq[Double]) extends Feature {
 

  def +(that: Numeric) = Numeric(typeName, this.zip(that).map { case (a, b) => a + b })
  def -(that: Numeric) = Numeric(typeName, this.zip(that).map { case (a, b) => a - b })
  def /(divisor: Double) = Numeric(typeName, terms.map(_ / divisor))
  def zip(that: Numeric) = this.terms.zip(that.terms)
  def dotProduct(that: Numeric) = this.zip(that).map { case (x, y) => x * y }.sum
  //Weighted dot product
  def WDotProduct(that: Numeric) = (this.terms, Numeric.weights.get(typeName).get, that.terms).zipped.map { case (x, w, y) => x * w * y }.sum
  
  def maxLimit(that:Numeric) : Numeric = Numeric(typeName,this.zip(that).map { case (a, b) => if (a>b) a else b })
  def minLimit(that:Numeric) : Numeric = Numeric(typeName,this.zip(that).map { case (a, b) => if (a<b) a else b })
  
  lazy val abs = Numeric(typeName, terms.map(_.abs))
  lazy val norm = math.sqrt(this.WDotProduct(this))
  lazy val numDimensions = terms.length
  lazy val sum = terms.sum
}

/**
 * A feature to represent IP address
 * @param ips a sequence of ips related to the same domain
 */
case class IP(val ips: Set[String]) extends Categorical {
  def maxLimit(that: IP) = this
  def minLimit(that: IP) = this
  def normalize(x: Double, max: Double, min: Double, newMax: Double, newMin: Double) = 
            ((x-min)/(max-min))*(newMax-newMin)+newMin
  
  // Similarity between two sets of IPS 
  def similarity(that: IP) = {
    val maxSize = 4
    //val c = math.sqrt( math.min(1, (this.ips.size + that.ips.size)/2*maxSize ))  
    val c = 1
    def compareIPSets(set1: Set[String], set2: Set[String]): Double = {

      def compareIP(s1: String, s2: String) = {
        val nets = s1.split("\\.").map( _.toInt).zip(s2.split("\\.").map( _.toInt))

        def loop(acc: Double, nets: Array[(Int, Int)]): Double = {

         
          def computeSim(numInterval : Int, x: Int, y:Int) : Double = {
            1.0 - normalize( math.abs(x-y), 256, 0, 1.0, 0) 
          }

          if (nets.isEmpty ) acc
          else if (nets.head._1 != nets.head._2) acc + computeSim(4,nets.head._1,nets.head._2)
          else loop(acc + 1, nets.tail)
        
        }

        normalize(loop(0, nets), 4,0,1,0)
      }
      var acc = 0.0
      for (ip1 <- set1) {
        for (ip2 <- set2)
          acc += compareIP(ip1, ip2)
      }
      (acc / set1.size + acc / set2.size) / 2
    }

    c * compareIPSets(this.ips, that.ips)
  }
}

/**
 *  A companion object to case class IP
 *  to support address translation from a url
 */
object IP {
  val maxSize = 4

  def lookupIp(host: String): List[String] = {
    val attributes = try {
      new InitialDirContext getAttributes ("dns:/%s" format host)
    } catch {
      case _: NamingException => return Nil
    }
    val list = {
      val attributeEnumeration = attributes.getAll
      var list = List[Attribute]()
      while (attributeEnumeration.hasMore)
        list = attributeEnumeration.next :: list
      attributeEnumeration.close
      list.reverse
    }
    list map (x => x.getID -> x.get.toString) flatMap {
      case ("A", x) => List(x)
      case ("CNAME", x) => lookupIp(x)
      case (_, x) => Nil
    }
  }
}
