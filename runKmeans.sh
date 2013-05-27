#!/bin/bash

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` {path to SPARK root} {0: to only run, 1: to package and run}"
  exit 1
fi

if [ $2 -eq 1 ]
then 
	$1/sbt/sbt assembly
fi
$1/sbt/sbt run

