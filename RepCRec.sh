#!/bin/bash

if [ "$1" = "" ]; then
	echo "python RepCRec_runner.py test/test1.txt"
	python RepCRec_runner.py test/test1.txt 
else
	echo "python RepCRec_runner.py ${1}"
	python RepCRec_runner.py $1
fi
