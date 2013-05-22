package main

import org.scalatest.FunSuite
import spark._
import spark.SparkContext._
import spark.util.Vector
import main.feature._
import main.distance._
import main.support._

class TestKmeans extends FunSuite {

  //TODO
  ignore("Test 1: read input") {
    val v = Support.parser("./in.txt", new SparkContext("local", "test1"))
    assert ( v.count == 8 )
  
  }


test("Weight distance on Numeric") {
	Numeric.weights = Map("space" -> List(1,0) )
	val n = List(   Numeric("space", List(1.0, 2.0)), 
					Numeric("space", List(1.0, 9.0)),
					Numeric("space", List(3.0, 2.0)),
					Numeric("space", List(4.0, 2.0))
				)
	Numeric.weights = Map("space" -> List(1,0) )
	assert ( NumericDistance("euclidean")(n(0),n(1)) == 0)
    
    Numeric.weights = Map("space" -> List(0,1) )
    assert ( NumericDistance("euclidean")(n(0),n(1)) != 0)   
}

}