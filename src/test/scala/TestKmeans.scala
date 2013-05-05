package main

import org.scalatest.FunSuite
import spark._
import spark.SparkContext._
import spark.util.Vector

class TestKmeans extends FunSuite {

  //TODO
  ignore("Test 1: read input") {
    val v = MultiSparkKmeans.parser("./in.txt", new SparkContext("local", "test1"))
    assert ( v.count == 8 )
  
  }

 ignore("Test 2") {
    assert(2 === 2)
  }


test("Weight distance on Numeric") {
	Numeric.weights = Map("space" -> List(1,0) )
	val n = List(   Numeric("space", List(1.0, 2.0)), 
					Numeric("space", List(1.0, 9.0)),
					Numeric("space", List(3.0, 2.0)),
					Numeric("space", List(4.0, 2.0))
				)
	Numeric.weights = Map("space" -> List(1,0) )
	assert ( Distance("euclidean")(n(0),n(1)) == 0)
    
    Numeric.weights = Map("space" -> List(0,1) )
    assert ( Distance("euclidean")(n(0),n(1)) != 0)   
}

}