#!/bin/bash

DATE=$(date +%Y%m%dT%H%M%S)

if [ $# -ne 4 ]
then
  echo "Usage: `basename $0` {path to SPARK root} {0: to only run, 1: to package and run} {sparkConf} {parametersFile}"
  exit 1
fi

if [ $2 -eq 1 ]
then 
	$1/sbt/sbt assembly
fi

if [ ! -d "out" ]; then
	mkdir out
fi

OUT=out/d$DATE
mkdir $OUT
RESULT=$OUT/out
LOG=$OUT/$DATE.log

$1/sbt/sbt "run $RESULT $3 $4" 2>&1 | tee $LOG