package main.feature

import javax.naming.directory.{ InitialDirContext, Attribute }
import javax.naming.NamingException
import scala.collection.JavaConversions._

import main.support.Normalize

/**
 *  An object for IP features
 *  to support address translation from a url
 *  to compute distance
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

  // Similarity between two sets of IPS 
  //set are represented as a string with - as separator
  def similarity(set1: String, set2: String) = {
    val maxSize = 4
    //val c = math.sqrt( math.min(1, (this.ips.size + that.ips.size)/2*maxSize ))  
    val c = 1
    def compareIPSets(set1: Set[String], set2: Set[String]): Double = {

      def compareIP(s1: String, s2: String) = {
        val nets = s1.split("\\.").map( _.toInt).zip(s2.split("\\.").map( _.toInt))

        def loop(acc: Double, nets: Array[(Int, Int)]): Double = {

         
          def computeSim(numInterval : Int, x: Int, y:Int) : Double = {
            1.0 - Normalize( math.abs(x-y), 256, 0, 1.0, 0) 
          }

          if (nets.isEmpty ) acc
          else if (nets.head._1 != nets.head._2) acc + computeSim(4,nets.head._1,nets.head._2)
          else loop(acc + 1, nets.tail)
        
        }

        Normalize(loop(0, nets), 4,0,1,0)
      }
      var acc = 0.0
      for (ip1 <- set1) {
        for (ip2 <- set2)
          acc += compareIP(ip1, ip2)
      }
      (acc / set1.size + acc / set2.size) / 2
    }
    c * compareIPSets(set1.split("-").toSet, set2.split("-").toSet)
  }
}
