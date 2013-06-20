Spark Kmeans for Spam 
========================
Implementation of Kmeans algorithm on [Spark](http://www.spark-project.org) system for clustering spam emails

# Getting Started #

## Requirements

[Spark 0.7.0](http://www.spark-project.org)  
Spark is an open source cluster computing system that aims to make data analytics fast — both fast to run and fast to write.

[sbt 0.11.3](http://www.scala-sbt.org/)  
A build tool for Scala and Java projects. It requires Java 1.6 or later.

[Scala 2.9.2](http://www.scala-lang.org/)  
Scala is a general purpose programming language designed to express common programming patterns in a concise, elegant, and type-safe way.


## Usage

Run **runKmeans.sh** with these required parameters:

* path to SPARK root  
* 0 to only run or 1 to package in jar and run (required to deploy on cluster nodes)
* spark configuration file (see below for format)
* parameter file  (see below for format) 
* outLabel optional label to add to output folder

E.g.  
```bash 
./runKmeans.sh /user/spark/spark-0.7.0 0 spark.conf parameter.conf kmeansOn1000spams
```  

## Configuration files

### spark.conf
Parameter | Meaning
---- | ----
**host** | master URL passed to Spark
**appName** | name of job
**inputfile** | a path to input json dataset, either a local one on the machine or a hdfs://
**evalOutput** | 1 to evaluate result, 0 otherwise

This is an example:

```json 
{
  "host": "spark://10.10.14.3:7077",
	"appName": "SparkKmeans",
	"inputFile": "hdfs://10.10.14.3:8020/user/spark/spam/spam10_6.json",
	"evalOutput": 1
}
```
Note: Master url must be follow these rules
Master URL | Meaning
---- | ----
local | Run Spark locally with one worker thread (i.e. no parallelism at all)
local[K] | Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine)
spark://HOST:PORT | Connect to the given Spark standalone cluster master


### parameter.conf

Parameter | Meaning
---- | ----
**mode** | method for kmeans, choose "PartialSums" or "Standard" (see [Overview on algorithm](https://bitbucket.org/bigfootproject/spark-kmeans/wiki/Overview%20on%20algorithm) )
**initialCentroids** | number of initial Centroids, a first approximation can be sqrt(inputSize/2)
**convergeDist** | Stopping condition: old and new centroids between iterations don't change more than this value.
**maxIter** | Stopping condition: iterations of the algorithm are limited to this value
**numSamplesForMedoid** | num of Elements in a cluster that will be used to update centroid for the categorical part in the Standard Kmeans (see [Overview on algorithm](https://bitbucket.org/bigfootproject/spark-kmeans/wiki/Overview%20on%20algorithm) )
**weights** | weights for features, values between 0 and 1. Sum of weights must be 1.

This is an example:

```json 
{
	"mode" : "PartialSums",
	"initialCentroids" : 100,
	"convergeDist" : 0.001,
	"maxIter" : 2,
	"numSamplesForMedoid" : 3,
	"weights" : {
		"space" : 0.20, 
		"time" : 0.20,
		"IP" : 0.20, 
		"uri" : 0.20, 
		"botname" : 0.20 
	}
}
```

## Output File
The output files are in folder **./out**  
Each execution has its output folder **./out[outLabel_]dDATE** with these following files:

File | Content
---- | ----
**out** | result Centroids 
**DATE.log** | spark log file of executed job
**parameter.conf** | file with algorithm parameters used to run the program




