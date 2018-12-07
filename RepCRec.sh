#!/bin/bash

# CSCI-GA 2434 Advanced Database Systems Final Project
# Replicated Concurrency Control and Recovery

# Team:
# Ren Yi--ry708@nyu.edu
# Yanqiu Wu--yw1370@nyu.edu


if [ "$1" = "" ]; then
	python RepCRec_runner.py test/test1.txt 
else
	python RepCRec_runner.py $1
fi
