#!/bin/bash

if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` {path to log folders root} "
  exit 1
fi


for i in $(ls $1);
do 
	echo ""
	echo ""
	echo "Trial $i "
	#cat $1/$i/*.log | grep "Job finished*"
	cat $1/$i/*.log | grep "Job finished*" | cut -d " " -f 11 | tr "." "," | awk '{s+=$1} END {print "Total time(s): " s}'
	cat $1/$i/*.log | tr -s "\n" | sed -n '/Begin QUALITY/,/End QUALITY/p' | sed '/Begin QUALITY/d' | sed '/End QUALITY/d'
	cat $1/$i/*.log | grep "Num of Iteration"
done