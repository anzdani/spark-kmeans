package main.distance
/**
 * A trait to represent a String distance function
 */

trait StringDistance extends ((String, String) => Double)

/**
 * A companion object to the StringDistance trait to select the
 * Distance corresponding to each string description
 */
object StringDistance {
  def apply(description: String) = description match {
    case "levenshtein" => Levenshtein
    case "normalizedLevenshtein" => NormalizedLevenshtein
    case _ => throw new MatchError("Invalid distance function: " + description)
  }
}

/**
 * Compute Levenshtein distance ( edit distance )
 */
object Levenshtein extends StringDistance {
   def minimum(i1: Int, i2: Int, i3: Int)= math.min(math.min(i1, i2), i3)
   def apply(s1:String, s2:String)={
      val dist=Array.tabulate(s2.length+1, s1.length+1){(j,i)=>if(j==0) i else if (i==0) j else 0}
 
      for(j<-1 to s2.length; i<-1 to s1.length)
         dist(j)(i)=if(s2(j-1)==s1(i-1)) dist(j-1)(i-1)
	            else minimum(dist(j-1)(i)+1, dist(j)(i-1)+1, dist(j-1)(i-1)+1)
 
      dist(s2.length)(s1.length)
   }
 }

/**
 * Compute Normalized Levenshtein distance ( edit distance )
 */
object NormalizedLevenshtein extends StringDistance {
   def apply(s1:String, s2:String)={
    val d = StringDistance("levenshtein")(s1,s2)
    val v = d / math.max(math.max(s1.size, s2.size),1)
    require(v <= 1, "Distance between 0 and 1")
    v
   }
}