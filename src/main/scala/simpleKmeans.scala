package main

object simpleKmeans {
  def main(args: Array[String]) {
    //parameter
    def k = 3
    def convergeDist = 0.1

    /** Input **/
    val points = Array.fill(100000) { Point.random }

    /** Initialization **/
    val centroids = Array.fill(k) { Point.random }

    val resultCentroids = kmeans(points, centroids, convergeDist)
    println(resultCentroids)
  }

  def kmeans(points: Seq[Point], centroids: Seq[Point], convergeDist: Double): Seq[Point] = {
    /** Assignnment Step **/
    //group points to closest centroid
    //output is < k, [v1,.,vn] >
    //k: centroid
    //[v1,.,vn]: closest point to centroids
    val pointGroups = points.groupBy(
      point => centroids.reduceLeft(
        //search for min distance
        (a, b) =>
          if ((point distance a) < (point distance b))
            a
          else
            b))

    /** Update Step **/
    // Recompute new centroids of each cluster as the average of the points in their cluster
    // case (centroid, pts)  is a partial function, it is equivalent 
    //to have t:(Point,Array[Point]) => t match { case (centroid, pts)
    val newCentroids = pointGroups.map({
      case (centroid, pts) => pts.reduceLeft(_ + _) / pts.length
    }).toSeq

    /** Stopping condition **/
    // Calculate the centroid movement
    val movement = (pointGroups zip newCentroids).map({
      case ((oldCentroid, pts), newCentroid) => oldCentroid distance newCentroid
    })

    // Repeat if movement exceeds threshold
    if (movement.exists(_ > convergeDist))
      kmeans(points, newCentroids, convergeDist)
    else
      return newCentroids
  }

}

class Point(myX: Double, myY: Double) {
  //members
  val x = myX
  val y = myY
  //relaxed operators
  def +(that: Point) = new Point(this.x + that.x, this.y + that.y)
  def -(that: Point) = this + (-that)
  def unary_-() = new Point(-this.x, -this.y)
  def /(d: Double) = new Point(this.x / d, this.y / d)
  //to define according to distance (Euclidean distance)
  def magnitude = math.sqrt(x * x + y * y)
  def distance(that: Point) = (that - this).magnitude

  override def toString = format("(%.2f - %.2f)", x, y)
}

object Point {
  //for random points
  def random() = new Point(math.random * 10, math.random * 10)
}