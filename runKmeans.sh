#!/bin/bash

DATE=$(date +%Y%m%dT%H%M%S)

if [ $# -lt 4 ]
then
  echo "Usage: `basename $0` {path to SPARK root} {0: to only run, 1: to package and run} {sparkConf} {parametersFile} [outLabel]"
  exit 1
fi

if [ $2 -eq 1 ]
then 
	$1/sbt/sbt assembly
fi

if [ ! -d "out" ]; then
	mkdir out
fi

OUT=out/
if [ $# -eq 5 ]
then
  OUT=$OUT$5_
fi

OUT=$OUT"d"$DATE
echo "Output will be in "$OUT
mkdir $OUT
RESULT=$OUT/out
LOG=$OUT/$DATE.log

cp ./parameter.conf $OUT/parameter.conf
$1/sbt/sbt "run $RESULT $3 $4" 2>&1 | tee $LOG